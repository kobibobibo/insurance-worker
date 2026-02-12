/**
 * Insurance Policy Processing Worker
 * Version: 2.1.0
 * 
 * Complete worker with clause-level citation extraction.
 * Polls Redis queue and processes policy documents through 6-stage pipeline.
 * 
 * Requirements compliance:
 * - Evidence-only output: No right appears without complete evidence
 * - Zero guessing on amounts: If schedule missing, amounts marked as unknown
 * - 6-stage pipeline: Intake → Map → Harvest → Normalize → Validate → Export
 * - Clause-level citations with document/page/section/quote
 */

const WORKER_VERSION = "2.2.0";

import { createClient } from '@supabase/supabase-js';
import { Redis } from '@upstash/redis';
import pdf from 'pdf-parse';
import { v4 as uuidv4 } from 'uuid';
import crypto from 'crypto';

// ============================================================
// CONFIGURATION
// ============================================================

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN });

const QUEUE_NAME = 'policy-processing';
const POLL_INTERVAL = 5000;
const MAX_RETRIES = 3;

// ============================================================
// CLAUSE EXTRACTION PATTERNS (from clauseExtraction.ts)
// ============================================================

const CLAUSE_PATTERNS = {
  hebrew: {
    section: /סעיף\s*(קטן\s*)?([\u05D0-\u05EAd\d][\d\.\u05D0-\u05EA]*)/g,
    chapter: /פרק\s*([\u05D0-\u05EA\d]+)/g,
    condition: /תנאי\s*([\d\.]+)/g,
    annex: /נספח\s*([\u05D0-\u05EA\d]+)/g,
    exclusion: /חריג\s*([\u05D0-\u05EA\d]+)/g,
    definition: /הגדרה\s*([\d\.]+)/g,
  },
  english: {
    section: /(?:Section|Sec\.?)\s*([\d]+(?:\.[\d]+)*(?:\.[a-z])?)/gi,
    clause: /(?:Clause|Cl\.?)\s*([\d]+(?:\.[\d]+)*(?:\.[a-z])?)/gi,
    article: /Article\s*([\d]+(?:\.[\d]+)*)/gi,
    paragraph: /(?:Paragraph|Para\.?)\s*([\d]+(?:\.[\d]+)*)/gi,
    appendix: /(?:Appendix|App\.?|Annex|Rider|Endorsement)\s*([A-Z\d]+)/gi,
    exclusion: /Exclusion\s*([\d]+)/gi,
    definition: /Definition\s*([\d]+(?:\.[\d]+)*)/gi,
  },
};

const HEADING_PATTERNS = {
  colonEnding: /^(.{3,60}):\s*$/m,
  hebrew: {
    common: /^(כיסוי|הגדרות|חריגים|תנאים|הוראות|זכויות|חובות|תביעות|ביטול|שינויים)\s/,
    numbered: /^[\u05D0-\u05EA\d]+[\.\)]\s+(.{3,50})$/m,
  },
  english: {
    allCaps: /^([A-Z][A-Z\s]{10,50})$/m,
    titleCase: /^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,6})$/m,
    numbered: /^[\d]+[\.\)]\s+([A-Z][A-Za-z\s]{3,50})$/m,
  },
};

const RIGHT_KEYWORDS = {
  hebrew: [
    'זכאי', 'זכאית', 'יהיה זכאי', 'תהיה זכאית',
    'כיסוי', 'יכוסה', 'מכוסה', 'יכסה',
    'החזר', 'יוחזר', 'להחזיר',
    'פיצוי', 'יפוצה', 'לפצות',
    'תשלום', 'ישולם', 'לשלם',
    'שיפוי', 'ישופה', 'לשפות',
    'יינתן', 'תינתן', 'ניתן ל',
    'רשאי', 'רשאית', 'מותר',
  ],
  english: [
    'entitled', 'shall be entitled', 'will be entitled',
    'covered', 'shall cover', 'will cover', 'coverage',
    'reimburse', 'reimbursement', 'shall reimburse',
    'compensate', 'compensation', 'shall compensate',
    'payment', 'shall pay', 'will pay',
    'indemnify', 'indemnification',
    'benefit', 'shall provide', 'will provide',
    'eligible', 'eligibility',
  ],
};

// Keywords indicating an exclusion (benefit NOT covered)
const EXCLUSION_KEYWORDS = {
  hebrew: [
    'לא יכוסה', 'אינו מכוסה', 'לא יהיה זכאי', 'לא תהיה זכאית',
    'חריג', 'חריגים', 'למעט', 'אינו כולל', 'לא כולל',
    'לא יינתן', 'לא ישולם', 'לא יוחזר', 'לא יפוצה',
    'פטור', 'פטורה', 'אין כיסוי', 'ללא כיסוי',
  ],
  english: [
    'not covered', 'not entitled', 'excluded', 'exclusion',
    'except', 'excluding', 'does not cover', 'will not cover',
    'shall not', 'will not', 'no coverage', 'not eligible',
    'waiver', 'exempt', 'not included', 'does not include',
  ],
};

// Keywords indicating monetary amounts (for schedule_required detection)
const AMOUNT_KEYWORDS = {
  hebrew: [
    '₪', 'ש"ח', 'שקל', 'שקלים',
    'סכום', 'תקרה', 'מקסימום', 'עד לסכום',
    'השתתפות עצמית', 'דמי השתתפות',
  ],
  english: [
    '$', '€', '£', 'USD', 'ILS', 'NIS',
    'amount', 'maximum', 'limit', 'up to',
    'deductible', 'copay', 'co-payment',
  ],
};

// Detect if text indicates an exclusion (not covered) vs inclusion (covered)
function detectBenefitStatus(text) {
  if (!text) return 'included';
  const lowerText = text.toLowerCase();
  
  // Check for exclusion indicators
  for (const keyword of [...EXCLUSION_KEYWORDS.hebrew, ...EXCLUSION_KEYWORDS.english]) {
    if (text.includes(keyword) || lowerText.includes(keyword)) {
      return 'excluded';
    }
  }
  
  return 'included';
}

// Detect if text contains amount references
function hasAmountReference(text) {
  if (!text) return false;
  const lowerText = text.toLowerCase();
  
  for (const keyword of [...AMOUNT_KEYWORDS.hebrew, ...AMOUNT_KEYWORDS.english]) {
    if (text.includes(keyword) || lowerText.includes(keyword)) {
      return true;
    }
  }
  
  // Also check for numeric patterns that look like amounts
  const amountPattern = /[\d,]+\.?\d*\s*(?:₪|ש"ח|שקל|\$|€|£)/;
  return amountPattern.test(text);
}

const ANNEX_PATTERNS = {
  hebrew: [/נספח/, /תוספת/, /רשימה/, /דף פרטים/, /הרחבה/],
  english: [/annex/i, /appendix/i, /rider/i, /endorsement/i, /schedule/i, /addendum/i, /supplement/i],
};

// ============================================================
// POLICY METADATA EXTRACTION PATTERNS
// ============================================================

const METADATA_PATTERNS = {
  // Policy number patterns — includes reversed RTL proximity patterns
  policyNumber: [
    /(?:מספר\s+פוליסה|פוליסה\s+מס(?:פר)?['"]?|פוליסה\s+מספר)[:\s]*([A-Z0-9\-\/]+)/i,
    /(?:policy\s*(?:no\.?|number|#))[:\s]*([A-Z0-9\-\/]+)/i,
    /פוליסה\s*[:\s]\s*(\d{6,15})/,
    /(?:מס['\.]?\s*פוליסה)[:\s]*([A-Z0-9\-\/]+)/i,
    // Reversed RTL: number appears near "פוליסה" or "לביטוח" within ~80 chars
    /(\d{6,15})\s+[^\n]{0,60}(?:פוליסה|לביטוח\s+פוליסה)/,
    /(?:פוליסה|לביטוח\s+פוליסה)[^\n]{0,60}\s+(\d{6,15})/,
    // "הסרג" (reversed "גרסה") near a number — common in OCR'd Hebrew
    /(\d{5,15})[\.\s]+הסרג/,
  ],
  // Insurer name patterns (Israeli insurers)
  insurerName: [
    /(הראל(?:\s+ביטוח)?)/,
    /(מגדל(?:\s+(?:חברה\s+)?ביטוח)?)/,
    /(כלל(?:\s+ביטוח)?)/,
    /(הפניקס(?:\s+ביטוח)?)/,
    /(מנורה(?:\s+מבטחים)?)/,
    /(איילון(?:\s+ביטוח)?)/,
    /(הכשרה(?:\s+ביטוח)?)/,
    /(שלמה(?:\s+ביטוח)?)/,
    /(ביטוח\s+ישיר)/,
    // Reversed OCR forms
    /(הרונמ)/, // reversed "מנורה"
    /(לדגמ)/, // reversed "מגדל"
    /(לארה)/, // reversed "הראל"
    /(AIG|Clal|Phoenix|Migdal|Harel|Menora)/i,
  ],
  // Policy type patterns - returns normalized type key
  // Added reversed RTL variants: "תואירב חוטיב" = reversed "ביטוח בריאות"
  policyType: [
    { pattern: /ביטוח\s+בריאות|בריאות\s+(?:פרט|ביטוח)|קבוצתי\s+בריאות|תואירב\s+חוטיב|health\s+insurance/i, type: 'health' },
    { pattern: /ביטוח\s+חיים|חיים\s+ומוות|life\s+insurance/i, type: 'life' },
    { pattern: /ביטוח\s+סיעודי|סיעוד|nursing\s+(?:care\s+)?insurance|long[- ]?term\s+care/i, type: 'nursing' },
    { pattern: /אובדן\s+כושר\s+עבודה|אובדן\s+כושר|אכ"ע|disability\s+insurance|income\s+protection/i, type: 'disability' },
    { pattern: /ביטוח\s+תאונות(?:\s+אישיות)?|תאונות\s+אישיות|accident\s+insurance|personal\s+accident/i, type: 'accident' },
    { pattern: /ביטוח\s+נסיעות|נסיעות\s+לחו"ל|travel\s+insurance/i, type: 'travel' },
    { pattern: /ביטוח\s+רכב|car\s+insurance|motor\s+insurance/i, type: 'car' },
    { pattern: /ביטוח\s+דירה|home\s+insurance/i, type: 'home' },
  ],
  // Start date patterns — standard + reversed RTL
  policyStartDates: [
    /(?:תאריך\s+(?:תחילת?\s+)?תוקף|מיום|תחילה)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    /(?:effective\s+(?:from|date))[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
    /(?:valid\s+from)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
    // "החל מ-01.04.2021" or "01.04.2021 - מה החל"
    /(?:החל\s+(?:מ[:\-\s]?|מיום\s+))(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    /(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})\s*[-–]\s*מה?\s+החל/,
    // Reversed: "date םוימ לחה" or "date מיום ... תחל"
    /(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})\s+םוימ\s+לחה/,
    /(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})\s+מיום\s+[^\n]{0,40}תחל/,
    // "from DATE" pattern in table-like contexts
    /(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})\s*(?:םוימ|מיום|מ-|מ\s)/,
  ],
  // End date patterns — standard + reversed RTL  
  policyEndDates: [
    /(?:תאריך\s+סיום|עד\s+ליום|תום\s+תוקף)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    // "עד 31.03.2026" or "31.03.2026 עד"
    /עד\s+(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    /(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})\s*(?:עד|דעו|ביום\s+לסיומה)/,
    // Reversed: "date םויל דעו"
    /(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})\s+םויל\s+דעו/,
  ],
  // Insured name patterns
  insuredName: [
    /(?:שם\s+(?:המבוטח|מבוטח)|מבוטח)[:\s]*([א-ת\s]{3,40})/,
    /(?:insured(?:\s+name)?)[:\s]*([A-Za-z\s]{3,40})/i,
    /(?:שם\s+מלא)[:\s]*([א-ת\s]{3,40})/,
  ],
};

// ============================================================
// CLAIM HISTORY EXTRACTION PATTERNS
// ============================================================

const CLAIM_PATTERNS = {
  // Claim number patterns
  claimNumber: [
    /(?:מספר\s+תביעה|תביעה\s+מס['"]?)[:\s]*([A-Z0-9\-\/]+)/i,
    /(?:claim\s*(?:no\.?|number|#|ref))[:\s]*([A-Z0-9\-\/]+)/i,
    /(?:אסמכתא|מס['\.]\s*אסמכתא)[:\s]*([A-Z0-9\-\/]+)/i,
    /(?:reference\s*(?:no\.?|number)?)[:\s]*([A-Z0-9\-\/]+)/i,
  ],
  // Claim submission date patterns
  claimDate: [
    /(?:תאריך\s+(?:הגשת?\s+)?(?:תביעה|הפנייה)|הוגש\s+ביום)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    /(?:date\s+(?:of\s+)?(?:claim|submission))[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
    /(?:submitted\s+on|filed\s+on)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
  ],
  // Denial date patterns
  denialDate: [
    /(?:תאריך\s+(?:הדחייה|דחייה)|נדחה\s+ביום|מכתב\s+דחייה\s+מיום)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    /(?:date\s+of\s+(?:denial|rejection))[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
    /(?:denied\s+on|rejected\s+on)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
  ],
  // Denial reason patterns - captures the text after the pattern
  denialReason: [
    /(?:סיבת\s+הדחייה|נימוק\s+הדחייה|נדחתה\s+(?:בשל|עקב|מהסיבה))[:\s]*([^\n]{10,200})/,
    /(?:reason\s+for\s+(?:denial|rejection))[:\s]*([^\n]{10,200})/i,
    /(?:your\s+claim\s+(?:was|has\s+been)\s+(?:denied|rejected)\s+(?:because|due\s+to))[:\s]*([^\n]{10,200})/i,
    /(?:לצערנו,?\s+(?:לא\s+נוכל\s+לאשר|איננו\s+יכולים\s+לאשר|התביעה\s+נדחית))[,\s]*(?:מאחר|היות|כיוון)\s+([^\n]{10,200})/,
  ],
  // Event date from claim forms
  eventDate: [
    /(?:תאריך\s+(?:האירוע|הניתוח|האשפוז|התאונה))[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    /(?:date\s+of\s+(?:event|incident|surgery|hospitalization|accident))[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
    /(?:occurred\s+on|happened\s+on)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
  ],
};

/**
 * Extract policy metadata from document text
 */
function extractPolicyMetadata(documents) {
  const metadata = {
    insurerName: "",
    policyNumber: "",
    policyType: "",
    policyStartDate: "",
    policyEndDate: "",
    insuredName: "",
    // Claim history fields
    claimNumber: "",
    claimDate: "",
    denialDate: "",
    denialReason: "",
    eventDate: "",
    extractedAt: new Date().toISOString(),
  };
  
  // Separate documents by type for targeted extraction
  const policyDocs = documents.filter(d => 
    ['policy', 'general_terms', 'schedule', 'endorsement'].includes(d.doc_type)
  );
  const claimDocs = documents.filter(d => 
    ['claim_form', 'correspondence', 'unknown'].includes(d.doc_type) || 
    d.display_name?.toLowerCase().includes('claim') ||
    d.display_name?.toLowerCase().includes('תביעה') ||
    d.display_name?.toLowerCase().includes('דחייה') ||
    d.display_name?.toLowerCase().includes('denial')
  );
  
  // Combine text from policy documents (focus on first few pages)
  const policyText = policyDocs
    .map(doc => {
      if (doc.page_texts && doc.page_texts.length > 0) {
        return doc.page_texts.slice(0, 5).join('\n');
      }
      return doc.text?.substring(0, 10000) || '';
    })
    .join('\n');
  
  // Combine text from claim-related documents
  const claimText = claimDocs
    .map(doc => doc.text || '')
    .join('\n');
  
  // Also search all documents if specific types not found
  const allText = documents
    .map(doc => {
      if (doc.page_texts && doc.page_texts.length > 0) {
        return doc.page_texts.slice(0, 5).join('\n');
      }
      return doc.text?.substring(0, 10000) || '';
    })
    .join('\n');
  
  const textToSearch = policyText || allText;
  
  if (!textToSearch && !claimText) return metadata;
  
  // Extract policy metadata from policy documents
  if (textToSearch) {
    // Extract insurer name
    for (const pattern of METADATA_PATTERNS.insurerName) {
      const match = textToSearch.match(pattern);
      if (match) {
        // Map reversed OCR forms back to correct names
        const reversedMap = { 'הרונמ': 'מנורה', 'לדגמ': 'מגדל', 'לארה': 'הראל' };
        const raw = match[1].trim();
        metadata.insurerName = reversedMap[raw] || raw;
        console.log(`[extractPolicyMetadata] Found insurer: ${metadata.insurerName}`);
        break;
      }
    }
    
    // Extract policy number with validation
    for (const pattern of METADATA_PATTERNS.policyNumber) {
      const match = textToSearch.match(pattern);
      if (match) {
        const candidate = match[1].trim();
        // Reject placeholders: "-", "0", "00", "N/A", single chars, etc.
        if (candidate.length >= 3 && !/^[-0]+$/.test(candidate) && !/^(N\/A|none|unknown)$/i.test(candidate)) {
          metadata.policyNumber = candidate;
          console.log(`[extractPolicyMetadata] Found policy number: ${metadata.policyNumber}`);
          break;
        }
      }
    }
    
    // Extract policy type - now uses object pattern with normalized type key
    for (const typePattern of METADATA_PATTERNS.policyType) {
      const match = textToSearch.match(typePattern.pattern);
      if (match) {
        metadata.policyType = typePattern.type;
        console.log(`[extractPolicyMetadata] Found policy type: ${typePattern.type} from match: ${match[0]}`);
        break;
      }
    }
    
    // Extract start date (separate from end date)
    for (const pattern of METADATA_PATTERNS.policyStartDates) {
      const match = textToSearch.match(pattern);
      if (match) {
        metadata.policyStartDate = formatDateForForm(match[1]);
        console.log(`[extractPolicyMetadata] Found start date: ${metadata.policyStartDate}`);
        break;
      }
    }
    
    // Extract end date
    for (const pattern of METADATA_PATTERNS.policyEndDates) {
      const match = textToSearch.match(pattern);
      if (match) {
        metadata.policyEndDate = formatDateForForm(match[1]);
        console.log(`[extractPolicyMetadata] Found end date: ${metadata.policyEndDate}`);
        break;
      }
    }
    
    // Extract insured name
    for (const pattern of METADATA_PATTERNS.insuredName) {
      const match = textToSearch.match(pattern);
      if (match) {
        const candidate = match[1].trim();
        // Validate: reject if too long (>30 chars), contains common non-name Hebrew words,
        // or has more than 4 words (real names are 2-4 words)
        const wordCount = candidate.split(/\s+/).filter(w => w.length > 0).length;
        const invalidNamePatterns = /(?:זכאים|כיסויים|בתחום|הרפואה|הפרט|ביטוח|פוליסה|תנאים|חריגים|הגדרות|המבטח|החברה|תביעה|סעיף|פרק|כיסוי|תשלום|שיפוי|החזר|נספח)/;
        if (wordCount >= 2 && wordCount <= 4 && candidate.length <= 30 && !invalidNamePatterns.test(candidate)) {
          metadata.insuredName = candidate;
          console.log(`[extractPolicyMetadata] Found insured name: ${metadata.insuredName}`);
          break;
        } else {
          console.log(`[extractPolicyMetadata] Rejected insuredName candidate: "${candidate}" (words=${wordCount}, len=${candidate.length})`);
        }
      }
    }
  }
  
  // Extract claim history from claim documents (or all docs as fallback)
  const claimSearchText = claimText || allText;
  if (claimSearchText) {
    // Extract claim number
    for (const pattern of CLAIM_PATTERNS.claimNumber) {
      const match = claimSearchText.match(pattern);
      if (match) {
        metadata.claimNumber = match[1].trim();
        console.log(`     📋 Found claim number: ${metadata.claimNumber}`);
        break;
      }
    }
    
    // Extract claim submission date
    for (const pattern of CLAIM_PATTERNS.claimDate) {
      const match = claimSearchText.match(pattern);
      if (match) {
        metadata.claimDate = formatDateForForm(match[1]);
        console.log(`     📋 Found claim date: ${metadata.claimDate}`);
        break;
      }
    }
    
    // Extract denial date
    for (const pattern of CLAIM_PATTERNS.denialDate) {
      const match = claimSearchText.match(pattern);
      if (match) {
        metadata.denialDate = formatDateForForm(match[1]);
        console.log(`     📋 Found denial date: ${metadata.denialDate}`);
        break;
      }
    }
    
    // Extract denial reason
    for (const pattern of CLAIM_PATTERNS.denialReason) {
      const match = claimSearchText.match(pattern);
      if (match) {
        // Clean up the reason text - remove extra whitespace and truncate
        metadata.denialReason = match[1].trim()
          .replace(/\s+/g, ' ')
          .substring(0, 300);
        console.log(`     📋 Found denial reason: ${metadata.denialReason.substring(0, 50)}...`);
        break;
      }
    }
    
    // Extract event date
    for (const pattern of CLAIM_PATTERNS.eventDate) {
      const match = claimSearchText.match(pattern);
      if (match) {
        metadata.eventDate = formatDateForForm(match[1]);
        console.log(`     📋 Found event date: ${metadata.eventDate}`);
        break;
      }
    }
  }
  
  return metadata;
}

/**
 * Convert date string to YYYY-MM-DD format for form input
 */
function formatDateForForm(dateStr) {
  if (!dateStr) return "";
  
  // Parse common date formats
  const patterns = [
    { regex: /(\d{1,2})[\/\.\-](\d{1,2})[\/\.\-](\d{4})/, order: [3, 2, 1] }, // DD/MM/YYYY
    { regex: /(\d{4})[\/\.\-](\d{1,2})[\/\.\-](\d{1,2})/, order: [1, 2, 3] }, // YYYY-MM-DD
    { regex: /(\d{1,2})[\/\.\-](\d{1,2})[\/\.\-](\d{2})/, order: [3, 2, 1], addCentury: true }, // DD/MM/YY
  ];
  
  for (const { regex, order, addCentury } of patterns) {
    const match = dateStr.match(regex);
    if (match) {
      let year = match[order[0]];
      let month = match[order[1]].padStart(2, '0');
      let day = match[order[2]].padStart(2, '0');
      
      if (addCentury && year.length === 2) {
        year = parseInt(year) > 50 ? '19' + year : '20' + year;
      }
      
      return `${year}-${month}-${day}`;
    }
  }
  
  return dateStr;
}

// ============================================================
// CLAUSE EXTRACTION FUNCTIONS
// ============================================================

function extractClauseReferences(text) {
  if (!text) return [];
  const references = [];
  
  // Process Hebrew patterns
  for (const [type, pattern] of Object.entries(CLAUSE_PATTERNS.hebrew)) {
    const regex = new RegExp(pattern.source, pattern.flags);
    let match;
    while ((match = regex.exec(text)) !== null) {
      references.push({
        type,
        number: match[2] || match[1],
        original: match[0],
        language: 'hebrew',
        position: match.index,
      });
    }
  }
  
  // Process English patterns
  for (const [type, pattern] of Object.entries(CLAUSE_PATTERNS.english)) {
    const regex = new RegExp(pattern.source, pattern.flags);
    let match;
    while ((match = regex.exec(text)) !== null) {
      references.push({
        type,
        number: match[1],
        original: match[0],
        language: 'english',
        position: match.index,
      });
    }
  }
  
  return references.sort((a, b) => a.position - b.position);
}

function findNearestClause(fullText, quote) {
  if (!fullText || !quote) return null;
  const quotePosition = fullText.indexOf(quote);
  if (quotePosition === -1) return null;
  
  const allRefs = extractClauseReferences(fullText);
  if (allRefs.length === 0) return null;
  
  let nearest = null;
  let nearestDistance = Infinity;
  
  for (const ref of allRefs) {
    if (ref.position <= quotePosition) {
      const distance = quotePosition - ref.position;
      if (distance < nearestDistance) {
        nearestDistance = distance;
        nearest = ref;
      }
    }
  }
  
  return nearest;
}

function formatClauseReference(ref) {
  if (!ref) return '';
  const typeLabels = {
    section: { he: 'סעיף', en: 'Section' },
    chapter: { he: 'פרק', en: 'Chapter' },
    clause: { he: 'סעיף', en: 'Clause' },
    article: { he: 'סעיף', en: 'Article' },
    paragraph: { he: 'פסקה', en: 'Paragraph' },
    appendix: { he: 'נספח', en: 'Appendix' },
    exclusion: { he: 'חריג', en: 'Exclusion' },
    definition: { he: 'הגדרה', en: 'Definition' },
    condition: { he: 'תנאי', en: 'Condition' },
    annex: { he: 'נספח', en: 'Annex' },
  };
  
  const label = typeLabels[ref.type] || { he: 'סעיף', en: 'Section' };
  return ref.language === 'hebrew' 
    ? `${label.he} ${ref.number}`
    : `${label.en} ${ref.number}`;
}

function extractHeadings(text) {
  if (!text) return [];
  const headings = [];
  const lines = text.split('\n');
  let position = 0;
  
  for (const line of lines) {
    const trimmed = line.trim();
    
    if (trimmed.length < 3) {
      position += line.length + 1;
      continue;
    }
    
    if (HEADING_PATTERNS.colonEnding.test(trimmed)) {
      headings.push({
        text: trimmed.replace(/:$/, '').trim(),
        level: 2,
        position,
        type: 'colon',
      });
    } else if (HEADING_PATTERNS.hebrew.numbered.test(trimmed)) {
      const match = trimmed.match(HEADING_PATTERNS.hebrew.numbered);
      if (match) {
        headings.push({
          text: match[1] || trimmed,
          level: 2,
          position,
          type: 'numbered',
        });
      }
    } else if (HEADING_PATTERNS.hebrew.common.test(trimmed)) {
      headings.push({
        text: trimmed,
        level: 1,
        position,
        type: 'common',
      });
    } else if (HEADING_PATTERNS.english.allCaps.test(trimmed)) {
      headings.push({
        text: trimmed,
        level: 1,
        position,
        type: 'caps',
      });
    } else if (HEADING_PATTERNS.english.numbered.test(trimmed)) {
      const match = trimmed.match(HEADING_PATTERNS.english.numbered);
      if (match) {
        headings.push({
          text: match[1],
          level: 2,
          position,
          type: 'numbered',
        });
      }
    }
    
    position += line.length + 1;
  }
  
  return headings;
}

function findNearestHeading(fullText, quote) {
  if (!fullText || !quote) return null;
  const quotePosition = fullText.indexOf(quote);
  if (quotePosition === -1) return null;
  
  const headings = extractHeadings(fullText.substring(0, quotePosition + 100));
  if (headings.length === 0) return null;
  
  const headingsBefore = headings.filter(h => h.position < quotePosition);
  return headingsBefore[headingsBefore.length - 1] || null;
}

function extractParagraphAnchor(text, wordCount = 8) {
  if (!text) return '';
  const words = text
    .replace(/[\n\r]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .filter(w => w.length > 0);
  
  return words.slice(0, wordCount).join(' ');
}

function findParagraphAnchor(fullText, quote) {
  if (!fullText || !quote) return null;
  const quotePosition = fullText.indexOf(quote);
  if (quotePosition === -1) return null;
  
  let paragraphStart = fullText.lastIndexOf('\n\n', quotePosition);
  if (paragraphStart === -1) paragraphStart = 0;
  else paragraphStart += 2;
  
  const paragraphText = fullText.substring(paragraphStart, quotePosition + quote.length);
  return extractParagraphAnchor(paragraphText);
}

function extractContext(fullText, quote, contextSize = 150) {
  if (!fullText || !quote) return '';
  const quotePosition = fullText.indexOf(quote);
  if (quotePosition === -1) return '';
  
  const beforeStart = Math.max(0, quotePosition - contextSize);
  const beforeText = fullText.substring(beforeStart, quotePosition);
  
  const sentenceStart = beforeText.lastIndexOf('.');
  const contextStart = sentenceStart !== -1 ? beforeStart + sentenceStart + 1 : beforeStart;
  
  const afterEnd = Math.min(fullText.length, quotePosition + quote.length + contextSize);
  const afterText = fullText.substring(quotePosition + quote.length, afterEnd);
  
  const sentenceEnd = afterText.indexOf('.');
  const contextEnd = sentenceEnd !== -1 
    ? quotePosition + quote.length + sentenceEnd + 1 
    : afterEnd;
  
  return fullText.substring(contextStart, contextEnd).trim();
}

function findRightConferringSentence(quote) {
  if (!quote) return null;
  const sentences = quote.split(/[.。]/);
  
  for (const sentence of sentences) {
    const trimmed = sentence.trim();
    if (trimmed.length < 10) continue;
    
    for (const keyword of RIGHT_KEYWORDS.hebrew) {
      if (trimmed.includes(keyword)) {
        return trimmed;
      }
    }
    
    const lowerSentence = trimmed.toLowerCase();
    for (const keyword of RIGHT_KEYWORDS.english) {
      if (lowerSentence.includes(keyword)) {
        return trimmed;
      }
    }
  }
  
  const firstSentence = sentences.find(s => s.trim().length > 20);
  return firstSentence?.trim() || null;
}

function isAnnexDocument(text) {
  if (!text) return false;
  for (const pattern of [...ANNEX_PATTERNS.hebrew, ...ANNEX_PATTERNS.english]) {
    if (pattern.test(text)) return true;
  }
  return false;
}

function extractAnnexName(text) {
  if (!text) return null;
  const patterns = [
    /נספח[:\s]+([^\n]{3,50})/,
    /תוספת[:\s]+([^\n]{3,50})/,
    /Annex\s*[A-Z\d]*[:\s]+([^\n]{3,50})/i,
    /Appendix\s*[A-Z\d]*[:\s]+([^\n]{3,50})/i,
    /Rider[:\s]+([^\n]{3,50})/i,
    /Endorsement[:\s]+([^\n]{3,50})/i,
  ];
  
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return match[1].trim();
    }
  }
  
  return null;
}

/**
 * Enrich an evidence span with clause-level citations
 */
function enrichEvidenceSpan(pageText, quote, documentId, documentName, documentType, page, confidence) {
  const clauseRef = findNearestClause(pageText || '', quote || '');
  const heading = findNearestHeading(pageText || '', quote || '');
  const paragraphAnchor = !clauseRef ? findParagraphAnchor(pageText || '', quote || '') : null;
  const context = extractContext(pageText || '', quote || '');
  const highlightedText = findRightConferringSentence(quote || '');
  const isAnnex = documentType === 'endorsement' || isAnnexDocument(documentName || '');
  const annexName = isAnnex ? extractAnnexName(pageText || '') || documentName : undefined;
  
  return {
    evidence_id: uuidv4(),
    document_id: documentId,
    page: page || 1,
    quote: quote || '',
    confidence: confidence || 0.8,
    section_path: clauseRef ? formatClauseReference(clauseRef) : undefined,
    document_name: documentName || 'Unknown Document',
    clause_number: clauseRef?.number,
    heading_title: heading?.text,
    paragraph_anchor: paragraphAnchor || undefined,
    excerpt_context: context || undefined,
    highlighted_text: highlightedText || undefined,
    is_annex: isAnnex,
    annex_name: annexName,
    verbatim: true,
  };
}

// ============================================================
// TEXT PROCESSING UTILITIES
// ============================================================

function normalizeHebrewText(text) {
  if (!text) return '';
  return text
    // Remove ALL control characters (0x00-0x1F) except tab (0x09), newline (0x0A), carriage return (0x0D)
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '')
    // Remove Unicode directional markers and BOM
    .replace(/[\u200E\u200F\u202A-\u202E\uFEFF]/g, '')
    // Normalize line endings
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .trim();
}

/**
 * Compute SHA256 hash of a buffer
 */
function computeSha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

/**
 * Generate policy fingerprint from all document hashes
 */
function generatePolicyFingerprint(documentHashes) {
  const combined = documentHashes.sort().join(':');
  return crypto.createHash('sha256').update(combined).digest('hex').substring(0, 16);
}

// ============================================================
// BENEFIT EXTRACTION
// ============================================================

const BENEFIT_KEYWORDS = {
  certain: [
    'זכאי', 'זכאית', 'יכוסה', 'מכוסה', 'יוחזר', 'יפוצה',
    'entitled', 'covered', 'reimbursed', 'compensated'
  ],
  conditional: [
    'בתנאי', 'אם', 'במקרה', 'כאשר', 'בכפוף',
    'if', 'when', 'provided that', 'subject to', 'conditional'
  ],
  service: [
    'שירות', 'סיוע', 'ייעוץ', 'תמיכה', 'מוקד',
    'service', 'assistance', 'support', 'helpline', 'concierge'
  ]
};

function detectBenefitLayer(text) {
  if (!text) return 'conditional';
  const lowerText = text.toLowerCase();
  
  // Check for service indicators first
  for (const keyword of BENEFIT_KEYWORDS.service) {
    if (text.includes(keyword) || lowerText.includes(keyword)) {
      return 'service';
    }
  }
  
  // Check for conditional indicators
  for (const keyword of BENEFIT_KEYWORDS.conditional) {
    if (text.includes(keyword) || lowerText.includes(keyword)) {
      return 'conditional';
    }
  }
  
  // Check for certain indicators
  for (const keyword of BENEFIT_KEYWORDS.certain) {
    if (text.includes(keyword) || lowerText.includes(keyword)) {
      return 'certain';
    }
  }
  
  return 'conditional';
}

/**
 * Extract amounts from text and determine their value state
 * @param {string} text - The text to extract amounts from
 * @param {boolean} hasSchedule - Whether a schedule document is present
 * @returns {object} Amounts object with values or unknown_schedule_required state
 */
function extractAmounts(text, hasSchedule) {
  const amounts = {
    value_state: hasSchedule ? 'known' : 'unknown_schedule_required',
    values: []
  };
  
  if (!text || !hasSchedule) {
    return amounts;
  }
  
  // Only extract amounts if we have a schedule
  const amountPatterns = [
    /(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*(?:₪|ש"ח|שקלים?)/g,
    /(?:up to|עד לסכום של?)\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)/gi,
    /(?:maximum|מקסימום|תקרה)\s*(?:of)?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)/gi,
  ];
  
  for (const pattern of amountPatterns) {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      amounts.values.push({
        raw: match[0],
        numeric: parseFloat(match[1].replace(/,/g, '')),
        position: match.index
      });
    }
  }
  
  return amounts;
}

/**
 * Generate a concise, human-readable benefit title from a raw policy paragraph.
 * Strips clause numbers, legal boilerplate prefixes, and extracts the core right description.
 */
function generateBenefitTitle(paragraph) {
  let text = paragraph;
  
  // Strip leading clause/section numbers: "15.1.3.", "סעיף 3.2", "א.", "1)", etc.
  text = text.replace(/^[\s]*(?:סעיף\s*)?[\d\u05D0-\u05EA]+(?:[.\-)\s]+[\d\u05D0-\u05EA]+)*[.\-)\s]+/, '');
  
  // Strip common legal boilerplate prefixes
  const boilerplatePrefixes = [
    /^הפוליסה\.\s*/,
    /^בכפוף\s+לתנאי\s+הפוליסה[,\s]*/,
    /^בהתאם\s+לתנאי\s+הפוליסה[,\s]*/,
    /^מוסכם\s+בזה\s+כי\s*/,
    /^הוסכם\s+כי\s*/,
    /^נקבע\s+כי\s*/,
    /^למען\s+הסר\s+ספק[,\s]*/,
    /^פסקה\s+/,
    /^כאמור\s+ב/,
  ];
  for (const prefix of boilerplatePrefixes) {
    text = text.replace(prefix, '');
  }
  
  text = text.trim();
  
  // Try to extract the core right by finding right-conferring verb phrases
  const rightPhrases = [
    /(?:זכ(?:אי|ות)\s+ל)(.{5,50}?)(?:[,.]|$)/,
    /(?:יהיה\s+זכאי\s+ל)(.{5,50}?)(?:[,.]|$)/,
    /(?:זכות\s+ל)(.{5,50}?)(?:[,.]|$)/,
    /(?:כיסוי\s+(?:ל|בגין)\s*)(.{5,50}?)(?:[,.]|$)/,
    /(?:פיצוי\s+(?:ל|בגין|בשל)\s*)(.{5,50}?)(?:[,.]|$)/,
    /(?:שיפוי\s+(?:ל|בגין|בשל)\s*)(.{5,50}?)(?:[,.]|$)/,
    /(?:החזר\s+(?:ל|בגין)\s*)(.{5,50}?)(?:[,.]|$)/,
    /(?:תגמולים?\s+(?:ל|בגין|בשל)\s*)(.{5,50}?)(?:[,.]|$)/,
    /(?:הודעה\s+(?:ל|בדבר|על)\s*)(.{5,50}?)(?:[,.]|$)/,
    /(?:ביטול|סיום|הפסקה|שינוי)\s+(?:ה)?(.{5,40}?)(?:[,.]|$)/,
  ];
  
  for (const pattern of rightPhrases) {
    const match = text.match(pattern);
    if (match && match[0]) {
      const phrase = match[0].trim().replace(/[,.]$/, '');
      if (phrase.length >= 10 && phrase.length <= 70) {
        return normalizeHebrewText(phrase);
      }
    }
  }
  
  // Try to find the first complete sentence (ends with period followed by space)
  const sentenceMatch = text.match(/^(.{15,70}?)\.\s/);
  if (sentenceMatch) {
    return normalizeHebrewText(sentenceMatch[1].trim());
  }
  
  // Fallback: extract key noun phrases describing the right
  // Look for common insurance topic markers
  const topicMarkers = [
    /(?:ביטוח\s+\S+(?:\s+\S+)?)/,
    /(?:דמי\s+ביטוח)/,
    /(?:תקופת\s+(?:ה)?(?:ביטוח|אכשרה|המתנה))/,
    /(?:בית\s+חולים\s+\S+)/,
    /(?:רופא\s+מומחה)/,
    /(?:טיפול\s+\S+)/,
    /(?:ניתוח\s+\S*)/,
    /(?:תרופות?\s+\S*)/,
    /(?:אשפוז\s+\S*)/,
  ];
  
  for (const marker of topicMarkers) {
    const match = text.match(marker);
    if (match) {
      // Build a title around this topic marker with some context
      const idx = text.indexOf(match[0]);
      const start = Math.max(0, text.lastIndexOf(' ', Math.max(0, idx - 15)) + 1);
      const end = Math.min(text.length, idx + match[0].length + 20);
      let candidate = text.substring(start, end);
      // Trim to word boundary
      const lastSp = candidate.lastIndexOf(' ');
      if (lastSp > 10) candidate = candidate.substring(0, lastSp);
      if (candidate.length >= 10 && candidate.length <= 60) {
        return normalizeHebrewText(candidate.trim());
      }
    }
  }
  
  // Last resort: first sentence fragment up to a natural break
  let fallback = text.substring(0, 60);
  const lastSpace = fallback.lastIndexOf(' ');
  if (lastSpace > 15) {
    fallback = fallback.substring(0, lastSpace);
  }
  return normalizeHebrewText(fallback.trim());
}

/**
 * Generate a concise summary (max ~200 chars) from a raw policy paragraph.
 * Strips clause numbers, trims to sentence boundaries, and avoids dumping raw text.
 */
function generateBenefitSummary(paragraph) {
  let text = paragraph;
  
  // Strip leading clause numbers
  text = text.replace(/^[\s]*(?:סעיף\s*)?[\d\u05D0-\u05EA]+(?:[.\-)\s]+[\d\u05D0-\u05EA]+)*[.\-)\s]+/, '');
  text = text.replace(/^הפוליסה\.\s*/, '');
  text = text.trim();
  
  // If short enough, return as-is
  if (text.length <= 200) {
    return normalizeHebrewText(text);
  }
  
  // Try to cut at a sentence boundary within first 200 chars
  const sentenceEnd = text.substring(0, 200).lastIndexOf('.');
  if (sentenceEnd > 80) {
    return normalizeHebrewText(text.substring(0, sentenceEnd + 1));
  }
  
  // Cut at last word boundary within 200 chars
  const lastSpace = text.substring(0, 200).lastIndexOf(' ');
  if (lastSpace > 80) {
    return normalizeHebrewText(text.substring(0, lastSpace) + '...');
  }
  
  return normalizeHebrewText(text.substring(0, 200) + '...');
}

/**
 * Extract benefits using paragraph-based chunking to keep complete thoughts together.
 * Instead of splitting on every sentence, we split on paragraph breaks (double newlines
 * or heading patterns) and keep each clause as a single benefit.
 */
function extractBenefits(text, documentId, pageTexts, displayName, docType, hasSchedule) {
  if (!text) return [];
  
  const benefits = [];
  
  // Split by paragraphs (double newlines, or numbered/lettered list items)
  // This keeps complete clauses together instead of fragmenting sentences
  const paragraphs = text.split(/\n\s*\n|(?=\n\s*[\u05D0-\u05EA\d]+[\.)\-]\s)|(?=\n\s*[a-zA-Z\d]+[\.)\-]\s)/);
  
  // Track found benefits to avoid exact duplicates
  const foundQuotes = new Set();
  
  for (let i = 0; i < paragraphs.length; i++) {
    const paragraph = paragraphs[i]?.trim();
    
    // Skip very short or very long paragraphs
    if (!paragraph || paragraph.length < 30 || paragraph.length > 2000) continue;
    
    // Check if this paragraph contains right-conferring language
    const hasRightKeyword = [...RIGHT_KEYWORDS.hebrew, ...RIGHT_KEYWORDS.english]
      .some(kw => paragraph.includes(kw) || paragraph.toLowerCase().includes(kw));
    
    if (!hasRightKeyword) continue;
    
    // Create a normalized key for duplicate detection (first 100 chars, normalized)
    const normalizedKey = paragraph.substring(0, 100)
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
    
    if (foundQuotes.has(normalizedKey)) continue;
    foundQuotes.add(normalizedKey);
    
    // Find which page this paragraph is on
    let page = 1;
    let pageText = text;
    if (pageTexts && Array.isArray(pageTexts)) {
      for (let p = 0; p < pageTexts.length; p++) {
        if (pageTexts[p] && pageTexts[p].includes(paragraph.substring(0, 50))) {
          page = p + 1;
          pageText = pageTexts[p];
          break;
        }
      }
    }
    
    // Create enriched evidence span
    const evidenceSpan = enrichEvidenceSpan(
      pageText,
      paragraph,
      documentId,
      displayName || 'Policy Document',
      docType || 'policy',
      page,
      0.85
    );
    
    // Use the evidence heading as the primary title source (it's the nearest
    // section heading and is almost always a clean, human-readable phrase).
    // Fall back to regex-based extraction only if no heading was found.
    const headingTitle = evidenceSpan.heading_title;
    const title = (headingTitle && headingTitle.length >= 5 && headingTitle.length <= 80)
      ? headingTitle
      : generateBenefitTitle(paragraph);
    
    // Detect if this is an included benefit or an exclusion
    const benefitId = uuidv4();
    const benefitStatus = detectBenefitStatus(paragraph);
    
    // Extract amounts with schedule awareness
    const amounts = hasAmountReference(paragraph) 
      ? extractAmounts(paragraph, hasSchedule) 
      : {};
    
    const benefit = {
      benefit_id: benefitId,
      layer: detectBenefitLayer(paragraph),
      title: normalizeHebrewText(title),
      summary: generateBenefitSummary(paragraph),
      status: benefitStatus,
      evidence_set: {
        spans: [evidenceSpan]
      },
      tags: [],
      eligibility: {},
      amounts: amounts,
      actionable_steps: []
    };
    
    benefits.push(benefit);
  }
  
  // Log summary
  const included = benefits.filter(b => b.status === 'included').length;
  const excluded = benefits.filter(b => b.status === 'excluded').length;
  console.log(`     📊 Benefits extracted: ${included} included, ${excluded} excluded`);
  
  return benefits;
}

// ============================================================
// MISSING REQUIREMENTS DETECTION
// ============================================================

/**
 * Check for missing requirements and return structured missing_requirements array
 */
function detectMissingRequirements(documents, processedDocs) {
  const missingRequirements = [];
  
  // Check for schedule
  const hasSchedule = documents.some(d => d.doc_type === 'schedule');
  if (!hasSchedule) {
    missingRequirements.push({
      code: 'schedule_required',
      severity: 'warning',
      message: 'לא זוהה דף פרטי ביטוח (Policy Schedule). סכומים ותקרות לא יוצגו.'
    });
  }
  
  // Check for policy document - now a warning, not a blocker
  // The system will still process any document and extract what it can
  const hasPolicy = documents.some(d => d.doc_type === 'policy');
  if (!hasPolicy) {
    missingRequirements.push({
      code: 'policy_not_found',
      severity: 'warning',
      message: 'לא זוהה מסמך פוליסה ראשי - הזכויות שיופקו עלולות להיות חלקיות.'
    });
  }
  
  // Check for unreadable documents
  for (const doc of documents) {
    const processed = processedDocs.find(p => p.document_id === doc.document_id);
    if (!processed || !processed.text || processed.text.length < 100) {
      missingRequirements.push({
        code: 'document_unreadable',
        severity: 'warning',
        message: `לא ניתן היה לקרוא את המסמך: ${doc.display_name}`,
        related_document_id: doc.document_id
      });
    }
  }
  
  return missingRequirements;
}

// ============================================================
// PIPELINE STAGES
// ============================================================

async function updateRunStatus(run_id, status, stage, extra = {}) {
  const { error } = await supabase
    .from('runs')
    .update({ 
      status, 
      stage, 
      updated_at: new Date().toISOString(),
      ...extra
    })
    .eq('run_id', run_id);
  
  if (error) {
    console.error(`     ⚠️ Failed to update run status: ${error.message}`);
  }
}

async function stageIntake(run_id) {
  console.log('  1️⃣  Intake - Fetching documents...');
  await updateRunStatus(run_id, 'queued', 'intake');
  
  // Fetch documents for this run
  const { data: documents, error } = await supabase
    .from('documents')
    .select('*')
    .eq('run_id', run_id);
  
  if (error) {
    throw new Error(`Failed to fetch documents: ${error.message}`);
  }
  
  if (!documents || documents.length === 0) {
    throw new Error('No documents found for this run');
  }
  
  const processedDocs = [];
  const documentHashes = [];
  
  // Check for schedule presence
  const hasSchedule = documents.some(d => d.doc_type === 'schedule');
  console.log(`     📋 Schedule document present: ${hasSchedule ? 'Yes' : 'No'}`);
  
  for (const doc of documents) {
    console.log(`     📄 Processing: ${doc.display_name || doc.storage_key}`);
    
    try {
      // Download PDF from storage
      const { data: fileData, error: downloadError } = await supabase.storage
        .from('policy-documents')
        .download(doc.storage_key);
      
      if (downloadError) {
        console.error(`     ⚠️ Failed to download ${doc.display_name}: ${downloadError.message}`);
        continue;
      }
      
      // Get buffer and compute SHA256
      const buffer = Buffer.from(await fileData.arrayBuffer());
      const sha256 = computeSha256(buffer);
      documentHashes.push(sha256);
      
      // Parse PDF
      const pdfData = await pdf(buffer);
      
      // Extract text per page
      const pageTexts = [];
      const pages = pdfData.text.split(/\f/); // Form feed typically separates pages
      for (const pageText of pages) {
        pageTexts.push(normalizeHebrewText(pageText));
      }
      
      processedDocs.push({
        document_id: doc.document_id,
        display_name: doc.display_name || 'Unknown Document',
        doc_type: doc.doc_type || 'policy',
        text: normalizeHebrewText(pdfData.text),
        page_texts: pageTexts.length > 0 ? pageTexts : [normalizeHebrewText(pdfData.text)],
        pages: pageTexts.length || 1,
        sha256: sha256
      });
      
      // Update document with page count (sha256 would need DB column)
      await supabase
        .from('documents')
        .update({ pages: pageTexts.length || 1 })
        .eq('document_id', doc.document_id);
        
    } catch (err) {
      console.error(`     ⚠️ Error processing ${doc.display_name}: ${err.message}`);
    }
  }
  
  // Generate policy fingerprint
  const policyFingerprint = generatePolicyFingerprint(documentHashes);
  console.log(`     🔐 Policy fingerprint: ${policyFingerprint}`);
  
  // Extract policy metadata for auto-fill
  const policyMetadata = extractPolicyMetadata(processedDocs);
  console.log(`     📋 Extracted metadata:`, {
    insurer: policyMetadata.insurerName || 'not found',
    policyNo: policyMetadata.policyNumber || 'not found',
    type: policyMetadata.policyType || 'not found',
  });
  
  // Detect missing requirements
  const missingRequirements = detectMissingRequirements(documents, processedDocs);
  if (missingRequirements.length > 0) {
    console.log(`     ⚠️ Missing requirements: ${missingRequirements.map(m => m.code).join(', ')}`);
  }
  
  // Update run with missing_requirements and policy_metadata
  await updateRunStatus(run_id, 'queued', 'intake', {
    missing_requirements: missingRequirements.map(m => m.code),
    policy_metadata: policyMetadata
  });
  
  console.log(`     ✓ Extracted ${processedDocs.length} documents`);
  
  return {
    documents: processedDocs,
    hasSchedule,
    policyFingerprint,
    missingRequirements,
    policyMetadata
  };
}

async function stageMap(run_id, intakeResult) {
  console.log('  2️⃣  Map - Analyzing structure...');
  await updateRunStatus(run_id, 'queued', 'map');
  
  const { documents } = intakeResult;
  
  if (!documents || !Array.isArray(documents)) {
    console.log('     ⚠️ No documents to map');
    return { ...intakeResult, sections: 0 };
  }
  
  let totalSections = 0;
  
  for (const doc of documents) {
    if (!doc.text) continue;
    
    // Extract headings to understand structure
    const headings = extractHeadings(doc.text);
    totalSections += headings.length;
    
    // Extract clause references
    const clauses = extractClauseReferences(doc.text);
    doc.clauses = clauses;
    doc.headings = headings;
  }
  
  console.log(`     ✓ Mapped ${totalSections} sections`);
  return { ...intakeResult, sections: totalSections };
}

async function stageHarvest(run_id, mapResult) {
  console.log('  3️⃣  Harvest - Extracting rights...');
  await updateRunStatus(run_id, 'queued', 'harvest');
  
  const { documents, hasSchedule } = mapResult;
  const benefits = [];
  
  if (!documents || !Array.isArray(documents)) {
    console.log('     ⚠️ No documents to harvest');
    return { ...mapResult, benefits };
  }
  
  for (const doc of documents) {
    if (!doc.text) continue;
    
    // Pass display_name, doc_type, and hasSchedule for enrichment
    const foundBenefits = extractBenefits(
      doc.text,
      doc.document_id,
      doc.page_texts,
      doc.display_name,
      doc.doc_type,
      hasSchedule
    );
    
    if (foundBenefits && Array.isArray(foundBenefits)) {
      benefits.push(...foundBenefits);
    }
  }
  
  // Count by layer
  const certain = benefits.filter(b => b.layer === 'certain').length;
  const conditional = benefits.filter(b => b.layer === 'conditional').length;
  const service = benefits.filter(b => b.layer === 'service').length;
  
  console.log(`     📊 Layers: certain=${certain}, conditional=${conditional}, service=${service}`);
  console.log(`     ✓ Harvested ${benefits.length} total benefits`);
  
  return { ...mapResult, benefits };
}

// Maximum benefits before triggering AI dedup (raised to avoid unnecessary AI calls)
const MAX_BENEFITS = 500;

// Supabase Edge Function URL for AI-powered deduplication
const DEDUPE_FUNCTION_URL = `${SUPABASE_URL}/functions/v1/dedupe-benefits`;

async function stageNormalize(run_id, harvestResult) {
  console.log('  4️⃣  Normalize - Standardizing and deduplicating...');
  await updateRunStatus(run_id, 'queued', 'normalize');
  
  const { benefits, hasSchedule } = harvestResult;
  
  if (!benefits || !Array.isArray(benefits)) {
    console.log('     ⚠️ No benefits to normalize');
    return { ...harvestResult, normalizedBenefits: [] };
  }
  
  console.log(`     📊 Raw benefits count: ${benefits.length}`);
  
  // Step 1: Basic normalization
  let normalizedBenefits = benefits.map((benefit) => {
    // Ensure amounts have proper value_state based on schedule presence
    let amounts = benefit.amounts || {};
    if (hasAmountReference(benefit.summary) && !hasSchedule) {
      amounts = {
        value_state: 'unknown_schedule_required',
        values: []
      };
    }
    
    return {
      benefit_id: benefit.benefit_id || uuidv4(),
      title: normalizeHebrewText(benefit.title || 'Untitled Benefit'),
      summary: normalizeHebrewText(benefit.summary || ''),
      layer: benefit.layer || 'conditional',
      status: benefit.status || 'included',
      evidence_set: benefit.evidence_set || { spans: [] },
      tags: benefit.tags || [],
      eligibility: benefit.eligibility || {},
      amounts: amounts,
      actionable_steps: benefit.actionable_steps || []
    };
  });
  
  // Step 2: Always run local fuzzy dedup first (fast, no network)
  const beforeFuzzy = normalizedBenefits.length;
  normalizedBenefits = fuzzyDeduplication(normalizedBenefits);
  if (normalizedBenefits.length < beforeFuzzy) {
    console.log(`     🔄 Fuzzy dedup: ${beforeFuzzy} -> ${normalizedBenefits.length}`);
  }
  
  // Step 3: Only call AI deduplication if still over the cap
  if (normalizedBenefits.length > MAX_BENEFITS) {
    console.log(`     🤖 Calling AI deduplication (${normalizedBenefits.length} -> max ${MAX_BENEFITS})...`);
    
    try {
      const response = await fetch(DEDUPE_FUNCTION_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
        },
        body: JSON.stringify({
          benefits: normalizedBenefits,
          maxBenefits: MAX_BENEFITS
        }),
      });
      
      if (response.ok) {
        const data = await response.json();
        if (data.benefits && Array.isArray(data.benefits)) {
          console.log(`     ✓ AI deduplication: ${normalizedBenefits.length} -> ${data.benefits.length} (${data.method})`);
          normalizedBenefits = data.benefits;
        }
      } else {
        console.warn(`     ⚠️ Deduplication failed (${response.status}), using fallback`);
        normalizedBenefits = fallbackDeduplication(normalizedBenefits, MAX_BENEFITS);
      }
    } catch (err) {
      console.warn(`     ⚠️ Deduplication error: ${err.message}, using fallback`);
      normalizedBenefits = fallbackDeduplication(normalizedBenefits, MAX_BENEFITS);
    }
  }
  
  console.log(`     ✓ Normalized ${normalizedBenefits.length} benefits`);
  return { ...harvestResult, normalizedBenefits };
}

/**
 * Fuzzy deduplication - catches near-duplicates by normalizing titles more aggressively
 * Runs locally, no network calls, very fast
 */
function fuzzyDeduplication(benefits) {
  const seen = new Map();
  
  for (const benefit of benefits) {
    // Aggressive normalization: strip punctuation, numbers, whitespace, common prefixes
    const key = (benefit.title || '')
      .replace(/[""״׳'`\-–—:;,.\u200F\u200E]/g, '')  // Remove punctuation & bidi marks
      .replace(/\d+/g, '')                              // Remove numbers
      .replace(/\s+/g, '')                               // Remove whitespace
      .toLowerCase()
      .replace(/^(כיסוי|זכות|שירות|הטבה|ביטוח)/g, '')   // Strip common Hebrew prefixes
      .substring(0, 40);
    
    if (!key) continue;
    
    if (!seen.has(key)) {
      seen.set(key, benefit);
    } else {
      // Merge: keep longer summary, merge evidence spans
      const existing = seen.get(key);
      if ((benefit.summary || '').length > (existing.summary || '').length) {
        existing.summary = benefit.summary;
      }
      const newSpans = benefit.evidence_set?.spans || [];
      existing.evidence_set.spans = [
        ...existing.evidence_set.spans,
        ...newSpans.filter(ns => 
          !existing.evidence_set.spans.some(es => es.quote === ns.quote)
        )
      ];
      // Merge tags
      if (benefit.tags?.length) {
        existing.tags = [...new Set([...(existing.tags || []), ...benefit.tags])];
      }
    }
  }
  
  // Cap evidence spans to max 5 per benefit for cleaner UI
  const MAX_EVIDENCE = 5;
  const results = Array.from(seen.values());
  for (const b of results) {
    if (b.evidence_set?.spans?.length > MAX_EVIDENCE) {
      b.evidence_set.spans = capEvidenceSpansWorker(b.evidence_set.spans, MAX_EVIDENCE);
    }
  }
  return results;
}

/**
 * Cap evidence spans: deduplicate by quote, then round-robin across documents for diversity.
 */
function capEvidenceSpansWorker(spans, max) {
  // Deduplicate
  const seen = new Set();
  const unique = spans.filter(s => {
    const q = (s.quote || '').trim();
    if (!q || seen.has(q)) return false;
    seen.add(q);
    return true;
  });
  if (unique.length <= max) return unique;

  // Group by document, sort by page within each
  const byDoc = new Map();
  for (const s of unique) {
    const docId = s.document_id || '_';
    if (!byDoc.has(docId)) byDoc.set(docId, []);
    byDoc.get(docId).push(s);
  }
  for (const arr of byDoc.values()) {
    arr.sort((a, b) => (a.page || 0) - (b.page || 0));
  }

  // Round-robin for diversity
  const result = [];
  const iters = Array.from(byDoc.values()).map(arr => ({ arr, idx: 0 }));
  while (result.length < max) {
    let added = false;
    for (const it of iters) {
      if (result.length >= max) break;
      if (it.idx < it.arr.length) { result.push(it.arr[it.idx]); it.idx++; added = true; }
    }
    if (!added) break;
  }
  return result;
}

/**
 * Fallback deduplication when AI is unavailable
 */
function fallbackDeduplication(benefits, maxCount) {
  const seen = new Map();
  
  for (const benefit of benefits) {
    // Create a normalized key from first 50 chars of title
    const key = (benefit.title || '')
      .substring(0, 50)
      .toLowerCase()
      .replace(/[^\u0590-\u05FFa-z0-9]/g, '');
    
    if (!seen.has(key)) {
      seen.set(key, benefit);
    } else {
      // Merge evidence spans from duplicate
      const existing = seen.get(key);
      const newSpans = benefit.evidence_set?.spans || [];
      existing.evidence_set.spans = [
        ...existing.evidence_set.spans,
        ...newSpans.filter(ns => 
          !existing.evidence_set.spans.some(es => es.quote === ns.quote)
        )
      ];
    }
    
    if (seen.size >= maxCount) break;
  }
  
  // Cap evidence spans
  const results = Array.from(seen.values());
  for (const b of results) {
    if (b.evidence_set?.spans?.length > 5) {
      b.evidence_set.spans = capEvidenceSpansWorker(b.evidence_set.spans, 5);
    }
  }
  return results;
}

async function stageValidate(run_id, normalizeResult) {
  console.log('  5️⃣  Validate - Checking quality...');
  await updateRunStatus(run_id, 'queued', 'validate');
  
  const { normalizedBenefits, missingRequirements } = normalizeResult;
  
  if (!normalizedBenefits || !Array.isArray(normalizedBenefits)) {
    console.log('     ⚠️ No benefits to validate');
    return { 
      ...normalizeResult, 
      validatedBenefits: { valid: [], invalid: [], score: 0 },
      qualityMetrics: { evidence_coverage_ratio: 0, benefits_count: 0, warnings: [] }
    };
  }
  
  // REQUIREMENT: 100% evidence coverage - filter benefits with valid evidence
  const benefitsWithEvidence = normalizedBenefits.filter(b => {
    const spans = b.evidence_set?.spans;
    return spans && Array.isArray(spans) && spans.length > 0 && 
           spans.every(s => s.quote && s.document_id && s.page);
  });
  
  // Benefits without complete evidence (rejected)
  const benefitsWithoutEvidence = normalizedBenefits.filter(b => {
    const spans = b.evidence_set?.spans;
    return !spans || !Array.isArray(spans) || spans.length === 0 ||
           !spans.every(s => s.quote && s.document_id && s.page);
  });
  
  // Calculate quality metrics
  const evidenceCoverageRatio = normalizedBenefits.length > 0 
    ? benefitsWithEvidence.length / normalizedBenefits.length 
    : 0;
  
  const warnings = [];
  
  // Add warnings based on missing requirements
  if (missingRequirements?.some(m => m.code === 'schedule_required')) {
    warnings.push('סכומים ותקרות לא מוצגים - חסר דף פרטי ביטוח');
  }
  
  if (benefitsWithoutEvidence.length > 0) {
    warnings.push(`${benefitsWithoutEvidence.length} זכויות נדחו עקב חוסר ראיות מלאות`);
  }
  
  const qualityMetrics = {
    evidence_coverage_ratio: evidenceCoverageRatio,
    benefits_count: benefitsWithEvidence.length,
    warnings
  };
  
  console.log(`     ✓ Valid: ${benefitsWithEvidence.length}, Invalid: ${benefitsWithoutEvidence.length}`);
  console.log(`     📊 Evidence coverage: ${(evidenceCoverageRatio * 100).toFixed(1)}%`);
  
  return {
    ...normalizeResult,
    validatedBenefits: {
      valid: benefitsWithEvidence,
      invalid: benefitsWithoutEvidence,
      score: evidenceCoverageRatio * 100
    },
    qualityMetrics
  };
}

async function stageExport(run_id, validateResult) {
  console.log('  6️⃣  Export - Saving to database...');
  await updateRunStatus(run_id, 'queued', 'export');
  
  const { validatedBenefits, qualityMetrics, documents, policyFingerprint } = validateResult;
  const benefits = validatedBenefits?.valid || [];
  
  if (!benefits || benefits.length === 0) {
    console.log('     ⚠️ No benefits to export');
    return { benefitCount: 0 };
  }
  
  // Insert benefits in batches of 50
  const batchSize = 50;
  let insertedCount = 0;
  
  for (let i = 0; i < benefits.length; i += batchSize) {
    const batch = benefits.slice(i, i + batchSize).map((b) => {
      // Validate status is one of allowed values
      const status = b.status === 'excluded' ? 'excluded' : 'included';
      return {
        benefit_id: b.benefit_id,
        run_id: run_id,
        layer: b.layer,
        title: b.title,
        summary: b.summary,
        status: status, // 'included' or 'excluded' per benefits_status_check constraint
        evidence_set: b.evidence_set,
        tags: b.tags,
        eligibility: b.eligibility,
        amounts: b.amounts,
        actionable_steps: b.actionable_steps
      };
    });
    
    const { error } = await supabase
      .from('benefits')
      .insert(batch);
    
    if (error) {
      console.error(`     ⚠️ Batch insert error: ${error.message}`);
      console.error(`     🔍 Error code: ${error.code}, hint: ${error.hint}, details: ${error.details}`);
    } else {
      insertedCount += batch.length;
    }
  }
  
  // Calculate total pages processed
  const totalPages = (documents || []).reduce((sum, d) => sum + (d.pages || 1), 0);
  
  // Update run with quality metrics
  const runQualityMetrics = {
    total_pages_processed: totalPages,
    extraction_confidence: qualityMetrics?.evidence_coverage_ratio || 0,
    validation_score: qualityMetrics?.evidence_coverage_ratio || 0
  };
  
  await supabase
    .from('runs')
    .update({ quality_metrics: runQualityMetrics })
    .eq('run_id', run_id);
  
  console.log(`     ✓ Exported ${insertedCount} benefits`);
  return { benefitCount: insertedCount };
}

// ============================================================
// MAIN PIPELINE
// ============================================================

async function processPolicyPipeline(run_id) {
  console.log(`\n📋 Starting pipeline for run: ${run_id}`);
  console.log('━'.repeat(50));
  
  try {
    // Stage 1: Intake
    const intakeResult = await stageIntake(run_id);
    if (!intakeResult.documents || intakeResult.documents.length === 0) {
      throw new Error('No documents could be processed');
    }
    
    // Check for blockers
    const blockers = intakeResult.missingRequirements?.filter(m => m.severity === 'blocker') || [];
    if (blockers.length > 0) {
      throw new Error(`Blocking issues: ${blockers.map(b => b.message).join('; ')}`);
    }
    
    // Stage 2: Map
    const mapResult = await stageMap(run_id, intakeResult);
    
    // Stage 3: Harvest
    const harvestResult = await stageHarvest(run_id, mapResult);
    
    // Stage 4: Normalize
    const normalizeResult = await stageNormalize(run_id, harvestResult);
    
    // Stage 5: Validate
    const validateResult = await stageValidate(run_id, normalizeResult);
    
    // Stage 6: Export
    const exportResult = await stageExport(run_id, validateResult);
    
    // Mark run as completed
    await updateRunStatus(run_id, 'completed', 'export');
    
    console.log('━'.repeat(50));
    console.log(`✅ Pipeline completed: ${exportResult.benefitCount} benefits extracted`);
    
    return { success: true, benefitCount: exportResult.benefitCount };
    
  } catch (error) {
    console.error(`\n❌ Pipeline failed: ${error.message}`);
    
    // Mark run as failed
    await supabase
      .from('runs')
      .update({ 
        status: 'failed', 
        stage: 'intake',
        error_message: error.message,
        updated_at: new Date().toISOString()
      })
      .eq('run_id', run_id);
    
    throw error;
  }
}

// ============================================================
// JOB PROCESSING
// ============================================================

async function processJob(job) {
  const { run_id, attempt = 1 } = job;
  
  console.log(`\n🔄 Processing job: ${run_id} (attempt ${attempt}/${MAX_RETRIES}) [Worker ${WORKER_VERSION}]`);
  
  try {
    // Verify run exists
    const { data: run, error: runError } = await supabase
      .from('runs')
      .select('*')
      .eq('run_id', run_id)
      .maybeSingle();
    
    if (runError) {
      throw new Error(`Failed to fetch run: ${runError.message}`);
    }
    
    if (!run) {
      console.log(`⚠️ Run ${run_id} not found, skipping`);
      return;
    }
    
    if (run.status === 'completed' || run.status === 'failed') {
      console.log(`⚠️ Run ${run_id} already ${run.status}, skipping`);
      return;
    }
    
    // Process the pipeline
    await processPolicyPipeline(run_id);
    
  } catch (error) {
    console.error(`❌ Job failed: ${run_id} ${error.message}`);
    
    if (attempt < MAX_RETRIES) {
      // Re-queue with incremented attempt
      console.log(`↩️ Re-queuing job (attempt ${attempt + 1})`);
      await redis.lpush(QUEUE_NAME, JSON.stringify({
        run_id,
        attempt: attempt + 1,
        queued_at: new Date().toISOString()
      }));
    } else {
      console.error(`💀 Job exhausted retries: ${run_id}`);
    }
    
    throw error;
  }
}

// ============================================================
// WORKER LOOP
// ============================================================

async function startWorker() {
  console.log('🚀 Insurance Worker Started');
  console.log(`   Queue: ${QUEUE_NAME}`);
  console.log(`   Poll Interval: ${POLL_INTERVAL}ms`);
  console.log(`   Max Retries: ${MAX_RETRIES}`);
  console.log('━'.repeat(50));
  
  while (true) {
    try {
      // Pop job from queue
      const jobData = await redis.rpop(QUEUE_NAME);
      
      if (jobData) {
        const job = typeof jobData === 'string' ? JSON.parse(jobData) : jobData;
        await processJob(job);
      } else {
        // No jobs, wait before polling again
        await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
      }
      
    } catch (error) {
      console.error(`Worker error: ${error.message}`);
      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
    }
  }
}

// Start the worker
startWorker().catch(console.error);
