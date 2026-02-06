/**
 * Insurance Policy Processing Worker
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
  // Policy number patterns
  policyNumber: [
    /(?:מספר פוליסה|פוליסה מס['"]?|פוליסה)[:\s]*([A-Z0-9\-\/]+)/i,
    /(?:policy\s*(?:no\.?|number|#))[:\s]*([A-Z0-9\-\/]+)/i,
    /פוליסה\s*[:\s]\s*(\d{6,15})/,
    /(?:מס['\.]?\s*פוליסה)[:\s]*([A-Z0-9\-\/]+)/i,
  ],
  // Insurer name patterns (Israeli insurers)
  insurerName: [
    /(הראל(?:\s+ביטוח)?)/,
    /(מגדל(?:\s+ביטוח)?)/,
    /(כלל(?:\s+ביטוח)?)/,
    /(הפניקס(?:\s+ביטוח)?)/,
    /(מנורה(?:\s+מבטחים)?)/,
    /(איילון(?:\s+ביטוח)?)/,
    /(הכשרה(?:\s+ביטוח)?)/,
    /(שלמה(?:\s+ביטוח)?)/,
    /(ביטוח\s+ישיר)/,
    /(AIG|Clal|Phoenix|Migdal|Harel|Menora)/i,
  ],
  // Policy type patterns
  policyType: [
    /(ביטוח\s+בריאות|health\s+insurance)/i,
    /(ביטוח\s+חיים|life\s+insurance)/i,
    /(ביטוח\s+סיעודי|nursing\s+(?:care\s+)?insurance)/i,
    /(אובדן\s+כושר\s+עבודה|disability\s+insurance)/i,
    /(ביטוח\s+תאונות(?:\s+אישיות)?|accident\s+insurance)/i,
    /(ביטוח\s+נסיעות|travel\s+insurance)/i,
    /(ביטוח\s+רכב|car\s+insurance|motor\s+insurance)/i,
    /(ביטוח\s+דירה|home\s+insurance)/i,
  ],
  // Date patterns (for policy start/end)
  policyDates: [
    /(?:תאריך\s+(?:תחילת?\s+)?תוקף|מיום|תחילה)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
    /(?:effective\s+(?:from|date))[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
    /(?:valid\s+from)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/i,
    /(?:תאריך\s+סיום|עד\s+ליום|תום\s+תוקף)[:\s]*(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})/,
  ],
  // Insured name patterns
  insuredName: [
    /(?:שם\s+(?:המבוטח|מבוטח)|מבוטח)[:\s]*([א-ת\s]{3,40})/,
    /(?:insured(?:\s+name)?)[:\s]*([A-Za-z\s]{3,40})/i,
    /(?:שם\s+מלא)[:\s]*([א-ת\s]{3,40})/,
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
    extractedAt: new Date().toISOString(),
  };
  
  // Combine text from all documents (focus on first few pages)
  const combinedText = documents
    .map(doc => {
      if (doc.page_texts && doc.page_texts.length > 0) {
        return doc.page_texts.slice(0, 5).join('\n');
      }
      return doc.text?.substring(0, 10000) || '';
    })
    .join('\n');
  
  if (!combinedText) return metadata;
  
  // Extract insurer name
  for (const pattern of METADATA_PATTERNS.insurerName) {
    const match = combinedText.match(pattern);
    if (match) {
      metadata.insurerName = match[1].trim();
      break;
    }
  }
  
  // Extract policy number
  for (const pattern of METADATA_PATTERNS.policyNumber) {
    const match = combinedText.match(pattern);
    if (match) {
      metadata.policyNumber = match[1].trim();
      break;
    }
  }
  
  // Extract policy type
  for (const pattern of METADATA_PATTERNS.policyType) {
    const match = combinedText.match(pattern);
    if (match) {
      metadata.policyType = match[1].trim();
      break;
    }
  }
  
  // Extract dates
  const dateMatches = [];
  for (const pattern of METADATA_PATTERNS.policyDates) {
    const match = combinedText.match(pattern);
    if (match) {
      dateMatches.push(match[1]);
    }
  }
  if (dateMatches.length > 0) {
    metadata.policyStartDate = formatDateForForm(dateMatches[0]);
    if (dateMatches.length > 1) {
      metadata.policyEndDate = formatDateForForm(dateMatches[1]);
    }
  }
  
  // Extract insured name
  for (const pattern of METADATA_PATTERNS.insuredName) {
    const match = combinedText.match(pattern);
    if (match) {
      metadata.insuredName = match[1].trim();
      break;
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

function extractBenefits(text, documentId, pageTexts, displayName, docType, hasSchedule) {
  if (!text) return [];
  
  const benefits = [];
  const sentences = text.split(/[.。\n]/);
  
  // Track found benefits to avoid duplicates
  const foundTitles = new Set();
  
  for (let i = 0; i < sentences.length; i++) {
    const sentence = sentences[i]?.trim();
    if (!sentence || sentence.length < 20) continue;
    
    // Check if this sentence contains right-conferring language
    const hasRightKeyword = [...RIGHT_KEYWORDS.hebrew, ...RIGHT_KEYWORDS.english]
      .some(kw => sentence.includes(kw) || sentence.toLowerCase().includes(kw));
    
    if (!hasRightKeyword) continue;
    
    // Generate a title from the sentence
    const title = sentence.substring(0, 80).trim();
    if (foundTitles.has(title)) continue;
    foundTitles.add(title);
    
    // Find which page this sentence is on
    let page = 1;
    let pageText = text;
    if (pageTexts && Array.isArray(pageTexts)) {
      for (let p = 0; p < pageTexts.length; p++) {
        if (pageTexts[p] && pageTexts[p].includes(sentence.substring(0, 50))) {
          page = p + 1;
          pageText = pageTexts[p];
          break;
        }
      }
    }
    
    // Create enriched evidence span
    const evidenceSpan = enrichEvidenceSpan(
      pageText,
      sentence,
      documentId,
      displayName || 'Policy Document',
      docType || 'policy',
      page,
      0.85
    );
    
    // Detect if this is an included benefit or an exclusion
    const benefitId = uuidv4();
    const benefitStatus = detectBenefitStatus(sentence);
    
    // Extract amounts with schedule awareness
    const amounts = hasAmountReference(sentence) 
      ? extractAmounts(sentence, hasSchedule) 
      : {};
    
    const benefit = {
      benefit_id: benefitId,
      layer: detectBenefitLayer(sentence),
      title: normalizeHebrewText(title),
      summary: normalizeHebrewText(sentence),
      status: benefitStatus, // 'included' or 'excluded' based on policy text
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
  
  // Check for policy document
  const hasPolicy = documents.some(d => d.doc_type === 'policy');
  if (!hasPolicy) {
    missingRequirements.push({
      code: 'policy_not_found',
      severity: 'blocker',
      message: 'לא זוהה מסמך פוליסה ראשי.'
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

async function stageNormalize(run_id, harvestResult) {
  console.log('  4️⃣  Normalize - Standardizing data...');
  await updateRunStatus(run_id, 'queued', 'normalize');
  
  const { benefits, hasSchedule } = harvestResult;
  
  if (!benefits || !Array.isArray(benefits)) {
    console.log('     ⚠️ No benefits to normalize');
    return { ...harvestResult, normalizedBenefits: [] };
  }
  
  // Normalize each benefit - preserve status from extraction
  const normalizedBenefits = benefits.map((benefit) => {
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
  
  console.log(`     ✓ Normalized ${normalizedBenefits.length} benefits`);
  return { ...harvestResult, normalizedBenefits };
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
  
  console.log(`\n🔄 Processing job: ${run_id} (attempt ${attempt}/${MAX_RETRIES})`);
  
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
