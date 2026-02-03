/**
 * Insurance Policy Processing Worker
 * 
 * Complete worker with clause-level citation extraction.
 * Polls Redis queue and processes policy documents through 6-stage pipeline.
 */

import { createClient } from '@supabase/supabase-js';
import { Redis } from '@upstash/redis';
import pdf from 'pdf-parse';
import { v4 as uuidv4 } from 'uuid';

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

const ANNEX_PATTERNS = {
  hebrew: [/נספח/, /תוספת/, /רשימה/, /דף פרטים/, /הרחבה/],
  english: [/annex/i, /appendix/i, /rider/i, /endorsement/i, /schedule/i, /addendum/i, /supplement/i],
};

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

function extractBenefits(text, documentId, pageTexts, displayName, docType) {
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
    
    // NOTE: status field is NOT included here - the database default will be used
    const benefit = {
      benefit_id: uuidv4(),
      layer: detectBenefitLayer(sentence),
      title: normalizeHebrewText(title),
      summary: normalizeHebrewText(sentence),
      evidence_set: {
        spans: [evidenceSpan]
      },
      tags: [],
      eligibility: {},
      amounts: {},
      actionable_steps: []
    };
    
    benefits.push(benefit);
  }
  
  return benefits;
}

// ============================================================
// PIPELINE STAGES
// ============================================================

async function updateRunStatus(run_id, status, stage) {
  const { error } = await supabase
    .from('runs')
    .update({ status, stage, updated_at: new Date().toISOString() })
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
      
      // Parse PDF
      const buffer = Buffer.from(await fileData.arrayBuffer());
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
        pages: pageTexts.length || 1
      });
      
      // Update document with page count
      await supabase
        .from('documents')
        .update({ pages: pageTexts.length || 1 })
        .eq('document_id', doc.document_id);
        
    } catch (err) {
      console.error(`     ⚠️ Error processing ${doc.display_name}: ${err.message}`);
    }
  }
  
  console.log(`     ✓ Extracted ${processedDocs.length} documents`);
  return processedDocs;
}

async function stageMap(run_id, documents) {
  console.log('  2️⃣  Map - Analyzing structure...');
  await updateRunStatus(run_id, 'queued', 'map');
  
  if (!documents || !Array.isArray(documents)) {
    console.log('     ⚠️ No documents to map');
    return { sections: 0, documents: [] };
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
  return { sections: totalSections, documents };
}

async function stageHarvest(run_id, documents, structure) {
  console.log('  3️⃣  Harvest - Extracting rights...');
  await updateRunStatus(run_id, 'queued', 'harvest');
  
  const benefits = [];
  
  if (!documents || !Array.isArray(documents)) {
    console.log('     ⚠️ No documents to harvest');
    return benefits;
  }
  
  for (const doc of documents) {
    if (!doc.text) continue;
    
    // Pass display_name and doc_type for enrichment
    const foundBenefits = extractBenefits(
      doc.text,
      doc.document_id,
      doc.page_texts,
      doc.display_name,
      doc.doc_type
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
  
  return benefits;
}

async function stageNormalize(run_id, benefits) {
  console.log('  4️⃣  Normalize - Standardizing data...');
  await updateRunStatus(run_id, 'queued', 'normalize');
  
  if (!benefits || !Array.isArray(benefits)) {
    console.log('     ⚠️ No benefits to normalize');
    return [];
  }
  
  // Normalize each benefit - NOTE: status is NOT set, database default will be used
  const normalizedBenefits = benefits.map(benefit => ({
    ...benefit,
    benefit_id: benefit.benefit_id || uuidv4(),
    title: normalizeHebrewText(benefit.title || 'Untitled Benefit'),
    summary: normalizeHebrewText(benefit.summary || ''),
    layer: benefit.layer || 'conditional',
    // status field intentionally omitted - database has default
    evidence_set: benefit.evidence_set || { spans: [] },
    tags: benefit.tags || [],
    eligibility: benefit.eligibility || {},
    amounts: benefit.amounts || {},
    actionable_steps: benefit.actionable_steps || []
  }));
  
  console.log(`     ✓ Normalized ${normalizedBenefits.length} benefits`);
  return normalizedBenefits;
}

async function stageValidate(run_id, benefits) {
  console.log('  5️⃣  Validate - Checking quality...');
  await updateRunStatus(run_id, 'queued', 'validate');
  
  if (!benefits || !Array.isArray(benefits)) {
    console.log('     ⚠️ No benefits to validate');
    return { valid: [], invalid: [], score: 0 };
  }
  
  // Filter benefits with valid evidence
  const benefitsWithEvidence = benefits.filter(b => {
    const spans = b.evidence_set?.spans;
    return spans && Array.isArray(spans) && spans.length > 0;
  });
  
  // Benefits without evidence
  const benefitsWithoutEvidence = benefits.filter(b => {
    const spans = b.evidence_set?.spans;
    return !spans || !Array.isArray(spans) || spans.length === 0;
  });
  
  const validationScore = benefits.length > 0 
    ? (benefitsWithEvidence.length / benefits.length) * 100 
    : 0;
  
  console.log(`     ✓ Valid: ${benefitsWithEvidence.length}, Invalid: ${benefitsWithoutEvidence.length}, Score: ${validationScore.toFixed(1)}%`);
  
  return {
    valid: benefitsWithEvidence,
    invalid: benefitsWithoutEvidence,
    score: validationScore
  };
}

async function stageExport(run_id, validatedBenefits, documents) {
  console.log('  6️⃣  Export - Saving to database...');
  await updateRunStatus(run_id, 'queued', 'export');
  
  const benefits = validatedBenefits?.valid || [];
  
  if (!benefits || benefits.length === 0) {
    console.log('     ⚠️ No benefits to export');
    return { benefitCount: 0 };
  }
  
  // Insert benefits in batches of 50
  const batchSize = 50;
  let insertedCount = 0;
  
  for (let i = 0; i < benefits.length; i += batchSize) {
    const batch = benefits.slice(i, i + batchSize).map(b => ({
      benefit_id: b.benefit_id,
      run_id: run_id,
      layer: b.layer,
      title: b.title,
      summary: b.summary,
      status: 'active', // REQUIRED: Must be 'active' or 'inactive' per benefits_status_check constraint
      evidence_set: b.evidence_set,
      tags: b.tags,
      eligibility: b.eligibility,
      amounts: b.amounts,
      actionable_steps: b.actionable_steps
    }));
    
    // Debug: log first benefit in batch to verify status is set
    if (i === 0) {
      console.log(`     📋 First benefit status: "${batch[0]?.status}"`);
    }
    
    const { error } = await supabase
      .from('benefits')
      .insert(batch);
    
    if (error) {
      console.error(`     ⚠️ Batch insert error: ${error.message}`);
      // Debug: log the actual data being sent
      console.error(`     📋 Sample benefit: ${JSON.stringify(batch[0], null, 2).substring(0, 500)}`);
    } else {
      insertedCount += batch.length;
    }
  }
  
  // Calculate quality metrics
  const totalPages = (documents || []).reduce((sum, d) => sum + (d.pages || 1), 0);
  const qualityMetrics = {
    total_pages_processed: totalPages,
    extraction_confidence: validatedBenefits?.score || 0,
    validation_score: validatedBenefits?.score || 0
  };
  
  // Update run with quality metrics
  await supabase
    .from('runs')
    .update({ quality_metrics: qualityMetrics })
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
    const documents = await stageIntake(run_id);
    if (!documents || documents.length === 0) {
      throw new Error('No documents could be processed');
    }
    
    // Stage 2: Map
    const structure = await stageMap(run_id, documents);
    
    // Stage 3: Harvest
    const benefits = await stageHarvest(run_id, documents, structure);
    
    // Stage 4: Normalize
    const normalizedBenefits = await stageNormalize(run_id, benefits);
    
    // Stage 5: Validate
    const validatedBenefits = await stageValidate(run_id, normalizedBenefits);
    
    // Stage 6: Export
    const exportResult = await stageExport(run_id, validatedBenefits, documents);
    
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
