/**
 * Insurance Policy Processing Worker
 * Consumes jobs from Upstash Redis queue and processes policy documents
 */

import { Redis } from '@upstash/redis';
import { createClient } from '@supabase/supabase-js';
import pdf from 'pdf-parse';
import { randomUUID } from 'crypto';

// Initialize clients
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// Configuration
const QUEUE_NAME = 'policy-processing';
const POLL_INTERVAL = 2000;
const MAX_RETRIES = 3;

// ═══════════════════════════════════════════════════════
// CLAUSE EXTRACTION UTILITIES (add after MAX_RETRIES)
// ═══════════════════════════════════════════════════════

const CLAUSE_PATTERNS = [
  /סעיף\s*([\d\.]+(?:\s*[א-ת])?)/gi,
  /פרק\s*([\d\.]+(?:\s*[א-ת])?)/gi,
  /section\s*([\d\.]+[a-z]?)/gi,
  /clause\s*([\d\.]+[a-z]?)/gi,
];

const RIGHT_KEYWORDS = ['זכאי', 'זכאית', 'מכוסה', 'ישולם', 'יכוסה', 'entitled', 'covered', 'payable'];

function extractClauseNumber(text, quotePosition) {
  let nearest = null;
  for (const pattern of CLAUSE_PATTERNS) {
    pattern.lastIndex = 0;
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const distance = Math.abs(match.index - quotePosition);
      if (!nearest || distance < nearest.distance) {
        nearest = { number: match[1].trim(), distance };
      }
    }
  }
  return nearest?.number;
}

function extractHeadingTitle(text, quotePosition) {
  const textBefore = text.substring(0, quotePosition);
  const lines = textBefore.split('\n').reverse();
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.endsWith(':') && trimmed.length > 5 && trimmed.length < 80) {
      return trimmed.slice(0, -1);
    }
    if (/^[א-ת\d\.]+\.\s+/.test(trimmed) && trimmed.length < 60) {
      return trimmed;
    }
  }
  return undefined;
}

function extractHighlightedText(text, quote) {
  const quoteStart = text.indexOf(quote);
  if (quoteStart === -1) return undefined;
  const contextStart = Math.max(0, quoteStart - 200);
  const contextEnd = Math.min(text.length, quoteStart + quote.length + 200);
  const sentences = text.substring(contextStart, contextEnd).split(/[.。]\s+/);
  for (const sentence of sentences) {
    if (RIGHT_KEYWORDS.some(kw => sentence.includes(kw)) && sentence.length > 20) {
      return sentence.trim() + '.';
    }
  }
  return undefined;
}

function extractExcerptContext(text, quote) {
  const quoteStart = text.indexOf(quote);
  if (quoteStart === -1) return quote;
  const start = Math.max(0, quoteStart - 100);
  const end = Math.min(text.length, quoteStart + quote.length + 100);
  let excerpt = text.substring(start, end);
  if (start > 0) excerpt = '...' + excerpt;
  if (end < text.length) excerpt += '...';
  return excerpt;
}


/**
 * Main worker loop - polls Redis queue for jobs
 */
async function startWorker() {
  console.log('🚀 Insurance Policy Worker Started');
  console.log('📋 Queue:', QUEUE_NAME);
  console.log('⏱️  Poll Interval:', POLL_INTERVAL, 'ms');
  console.log('🔄 Max Retries:', MAX_RETRIES);
  console.log('─'.repeat(50));

  while (true) {
    try {
      const jobData = await redis.lpop(QUEUE_NAME);

      if (jobData) {
        const job = typeof jobData === 'string' ? JSON.parse(jobData) : jobData;
        console.log(`\n📥 New job received:`, job);
        await processJob(job);
      } else {
        await sleep(POLL_INTERVAL);
      }
    } catch (error) {
      console.error('❌ Worker error:', error);
      await sleep(POLL_INTERVAL);
    }
  }
}

/**
 * Process a single job
 */
async function processJob(job) {
  const { run_id, retry_count = 0 } = job;

  try {
    console.log(`\n🔧 Checking run: ${run_id}`);

    const { data: existingRun, error: fetchError } = await supabase
      .from('runs')
      .select('status')
      .eq('run_id', run_id)
      .maybeSingle();
    
    if (fetchError) {
      console.error(`❌ Failed to fetch run status:`, fetchError);
      throw fetchError;
    }

    if (!existingRun) {
      console.log(`⚠️ Run not found: ${run_id} - may have been deleted`);
      return; // Skip this job
    }

    if (existingRun?.status === 'completed') {
      console.log(`⏭️  Skipping already completed run: ${run_id}`);
      return;
    }

    console.log(`🔧 Processing run: ${run_id}`);
    await updateRunStatus(run_id, 'running', 'intake');
    await processPolicyPipeline(run_id);
    await updateRunStatus(run_id, 'completed', 'export');
    console.log(`✅ Job completed: ${run_id}`);

  } catch (error) {
    console.error(`❌ Job failed: ${run_id}`, error);

    if (retry_count < MAX_RETRIES) {
      console.log(`🔄 Retrying job (attempt ${retry_count + 1}/${MAX_RETRIES})`);
      await redis.rpush(QUEUE_NAME, JSON.stringify({
        ...job,
        retry_count: retry_count + 1
      }));
    } else {
      await updateRunStatus(run_id, 'failed', 'intake', {
        error_code: 'max_retries_exceeded',
        error_message: error.message
      });
    }
  }
}

/**
 * Main processing pipeline
 */
async function processPolicyPipeline(run_id) {
  console.log('\n📊 Pipeline Stages:');

  // Stage 1: Intake
  console.log('  1️⃣  Intake - Fetching documents...');
  await updateRunStatus(run_id, 'running', 'intake');
  const documents = await stageIntake(run_id);
  console.log(`     ✓ Extracted ${documents.length} documents`);

  // Stage 2: Map
  console.log('  2️⃣  Map - Analyzing structure...');
  await updateRunStatus(run_id, 'running', 'map');
  const structure = await stageMap(run_id, documents);
  console.log(`     ✓ Mapped ${Object.keys(structure).length} sections`);

  // Stage 3: Harvest
  console.log('  3️⃣  Harvest - Extracting rights...');
  await updateRunStatus(run_id, 'running', 'harvest');
  const benefits = await stageHarvest(run_id, documents, structure);
  console.log(`     ✓ Found ${benefits.length} benefits`);

  // Stage 4: Normalize
  console.log('  4️⃣  Normalize - Standardizing data...');
  await updateRunStatus(run_id, 'running', 'normalize');
  const normalized = await stageNormalize(benefits);
  console.log(`     ✓ Normalized ${normalized.length} benefits`);

  // Stage 5: Validate
  console.log('  5️⃣  Validate - Verifying evidence...');
  await updateRunStatus(run_id, 'running', 'validate');
  const validated = await stageValidate(run_id, normalized);
  console.log(`     ✓ Validated with ${validated.coverage_ratio * 100}% evidence coverage`);

  // Stage 6: Export
  console.log('  6️⃣  Export - Generating exports...');
  await updateRunStatus(run_id, 'running', 'export');
  await stageExport(run_id);
  console.log(`     ✓ Exports ready`);

  console.log('\n✨ Pipeline completed successfully!\n');
}

/**
 * Stage 1: Intake - Fetch and extract document text
 */
async function stageIntake(run_id) {
  const { data: docs, error } = await supabase
    .from('documents')
    .select('*')
    .eq('run_id', run_id);

  if (error) throw error;
  if (!docs || docs.length === 0) {
    throw new Error('No documents found for this run');
  }

  const extractedDocs = [];

  for (const doc of docs) {
    console.log(`     📄 Processing: ${doc.display_name}`);

    const { data: fileData, error: downloadError } = await supabase
      .storage
      .from('policy-documents')
      .download(doc.storage_key);

    if (downloadError) {
      console.error(`     ⚠️  Failed to download: ${doc.display_name}`);
      continue;
    }

    const buffer = await fileData.arrayBuffer();
    const pdfData = await pdf(Buffer.from(buffer));

    extractedDocs.push({
      ...doc,
      text: pdfData.text,
      num_pages: pdfData.numpages,
      page_texts: extractPageTexts(pdfData.text, pdfData.numpages)
    });
  }

  return extractedDocs;
}

/**
 * Stage 2: Map - Analyze document structure
 */
async function stageMap(run_id, documents) {
  const structure = {};

  for (const doc of documents) {
    const sections = detectSections(doc.text);
    
    structure[doc.document_id] = {
      doc_type: doc.doc_type,
      sections: sections,
      has_schedule: detectSchedule(doc.text, doc.display_name)
    };
  }

  const hasSchedule = Object.values(structure).some(s => s.has_schedule);
  
  if (!hasSchedule) {
    await supabase
      .from('runs')
      .update({
        missing_requirements: [{
          code: 'schedule_required',
          severity: 'warning',
          message: 'נספח פרטי הביטוח חסר - סכומים לא יוצגו'
        }]
      })
      .eq('run_id', run_id);
  }

  return structure;
}

/**
 * Stage 3: Harvest - Extract benefits with evidence
 */
async function stageHarvest(run_id, documents, structure) {
  const benefits = [];

  for (const doc of documents) {
    // Pass display_name and doc_type for enrichment
    const foundBenefits = extractBenefits(
      doc.text, 
      doc.document_id, 
      doc.page_texts,
      doc.display_name,  // NEW
      doc.doc_type       // NEW
    );
    benefits.push(...foundBenefits);
  }

  // ... rest unchanged
}


/**
 * Stage 4: Normalize - Standardize data
 */
async function stageNormalize(benefits) {
  return benefits.map(benefit => ({
    ...benefit,
    title: normalizeHebrewText(benefit.title),
  }));
}

/**
 * Stage 5: Validate - Check evidence coverage
 */
async function stageValidate(run_id, benefits) {
  const benefitsWithEvidence = benefits.filter(
    b => b.evidence_set && b.evidence_set.spans && b.evidence_set.spans.length > 0
  );

  const coverage_ratio = benefits.length > 0 
    ? benefitsWithEvidence.length / benefits.length 
    : 0;

  await supabase
    .from('runs')
    .update({
      quality_metrics: {
        evidence_coverage_ratio: coverage_ratio,
        benefits_count: benefits.length,
        layer_distribution: calculateLayerDistribution(benefits),
        warnings: coverage_ratio < 1.0 ? ['Some benefits missing evidence'] : []
      }
    })
    .eq('run_id', run_id);

  if (coverage_ratio < 1.0) {
    throw new Error('Evidence coverage validation failed');
  }

  return { coverage_ratio };
}

/**
 * Stage 6: Export - Generate output files
 */
async function stageExport(run_id) {
  console.log('     📦 Export generation (placeholder)');
}

/**
 * Helper: Update run status in database
 */
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
    console.error('Failed to update run status:', error);
  }
}

/**
 * Helper: Extract page-level text from PDF
 */
function extractPageTexts(fullText, numPages) {
  const avgCharsPerPage = Math.ceil(fullText.length / numPages);
  const pages = [];
  
  for (let i = 0; i < numPages; i++) {
    const start = i * avgCharsPerPage;
    const end = start + avgCharsPerPage;
    pages.push(fullText.substring(start, end));
  }
  
  return pages;
}

/**
 * Helper: Detect sections in Hebrew text
 */
function detectSections(text) {
  const sectionPattern = /(?:חלק|סעיף|פרק)\s+[א-ת']+/g;
  const matches = text.match(sectionPattern) || [];
  return matches.map(m => ({ title: m, type: 'section' }));
}

/**
 * Helper: Detect if document is a schedule
 */
function detectSchedule(text, filename) {
  const scheduleKeywords = ['נספח', 'פרטי הביטוח', 'לוח תגמולים', 'schedule'];
  const lowerText = text.toLowerCase();
  const lowerFilename = filename.toLowerCase();
  
  return scheduleKeywords.some(keyword => 
    lowerText.includes(keyword) || lowerFilename.includes(keyword)
  );
}

/**
 * Helper: Classify benefit layer based on text content
 * Returns: 'certain' | 'conditional' | 'service'
 */
function classifyBenefitLayer(text) {
  const lowerText = text.toLowerCase();
  
  // Conditional indicators - benefits that require conditions/approvals/waiting periods
  const conditionalKeywords = [
    // Hebrew
    'בכפוף ל',
    'באישור',
    'לאחר תקופת המתנה',
    'בתנאי ש',
    'מותנה',
    'תקופת אכשרה',
    'לאחר אישור',
    'טעון אישור',
    'באישור מראש',
    'בכפוף לאישור',
    'תקופת המתנה',
    'לאחר',
    'ימי המתנה',
    'חודשי המתנה',
    'אם וכאשר',
    'בהתקיים',
    'ובלבד ש',
    'למעט',
    'אלא אם',
    'בהתאם לשיקול דעת',
    'עד לתקרה',
    'מקסימום',
    'עד',
    'לא יותר מ',
    // English
    'subject to',
    'pending approval',
    'waiting period',
    'conditional',
    'upon approval',
    'pre-authorization',
    'requires approval',
    'after',
    'maximum',
    'up to',
    'not exceeding',
    'provided that',
    'unless',
    'except'
  ];
  
  // Service indicators - assistance/support services (not direct payments)
  const serviceKeywords = [
    // Hebrew
    'שירות',
    'סיוע',
    'ייעוץ',
    'מוקד',
    'תמיכה',
    'הנחה',
    'הטבה',
    'שירותי רווחה',
    'קו חם',
    'מידע',
    'הכוונה',
    'ליווי',
    'תיאום',
    'הפניה',
    'ייעוץ טלפוני',
    'שירות לקוחות',
    'מוקד שירות',
    'סיוע טכני',
    'תמיכה נפשית',
    'ייעוץ רפואי',
    'חוות דעת שנייה',
    'second opinion',
    // English
    'service',
    'assistance',
    'helpline',
    'support',
    'hotline',
    'consultation',
    'guidance',
    'coordination',
    'referral',
    'advisory',
    'concierge',
    'wellness',
    'discount',
    'benefit program'
  ];
  
  // Check for service indicators first (most specific)
  if (serviceKeywords.some(kw => lowerText.includes(kw))) {
    return 'service';
  }
  
  // Check for conditional indicators
  if (conditionalKeywords.some(kw => lowerText.includes(kw))) {
    return 'conditional';
  }
  
  // Default to certain (guaranteed rights)
  return 'certain';
}

/**
 * Helper: Find the page number where a quote appears
 */
function findPageForQuote(quote, pageTexts) {
  if (!pageTexts || pageTexts.length === 0) return 1;
  
  const normalizedQuote = quote.substring(0, 50).toLowerCase();
  
  for (let i = 0; i < pageTexts.length; i++) {
    if (pageTexts[i].toLowerCase().includes(normalizedQuote)) {
      return i + 1; // Pages are 1-indexed
    }
  }
  
  return 1; // Default to page 1 if not found
}



function extractBenefits(text, document_id, pageTexts = [], docDisplayName = '', docType = 'policy') {
  const benefits = [];
  const seenQuotes = new Set();
  const isAnnex = docType === 'endorsement' || docType === 'schedule';
  
  const benefitPatterns = [
    /(?:זכאי ל|זכאים ל)([^.。\n]+)/g,
    /(?:מכסה|מכוסה)([^.。\n]+)/g,
    /(?:שיפוי|פיצוי)([^.。\n]+)/g,
    /(?:תגמול|תגמולים)([^.。\n]+)/g,
    /(?:החזר|החזרים)([^.。\n]+)/g,
    /(?:כיסוי ל|כיסוי עבור)([^.。\n]+)/g,
    /(?:ישולם|ישולמו)([^.。\n]+)/g,
    /(?:יקבל|יקבלו)([^.。\n]+)/g,
  ];
  
  for (const pattern of benefitPatterns) {
    let match;
    pattern.lastIndex = 0;
    
    while ((match = pattern.exec(text)) !== null) {
      const fullMatch = match[0];
      const quote = fullMatch.substring(0, 300).trim();
      const quoteKey = quote.substring(0, 80);
      
      if (seenQuotes.has(quoteKey) || quote.length < 20) continue;
      seenQuotes.add(quoteKey);
      
      const layer = classifyBenefitLayer(quote);
      const page = findPageForQuote(quote, pageTexts);
      const pageText = pageTexts[page - 1] || text;
      const quotePosition = pageText.indexOf(quote);
      
      let title = quote.substring(0, 80);
      const lastSpace = title.lastIndexOf(' ');
      if (lastSpace > 40) title = title.substring(0, lastSpace);
      
      benefits.push({
        benefit_id: randomUUID(),
        layer: layer,
        title: title,
        summary: quote,
        status: 'included',
        evidence_set: {
          spans: [{
            evidence_id: randomUUID(),
            document_id,
            page: page,
            quote: quote,
            verbatim: true,
            confidence: 0.8,
            // NEW ENRICHED FIELDS:
            document_name: isAnnex ? `נספח: ${docDisplayName}` : 'פוליסה ראשית',
            clause_number: extractClauseNumber(pageText, quotePosition),
            heading_title: extractHeadingTitle(pageText, quotePosition),
            excerpt_context: extractExcerptContext(pageText, quote),
            highlighted_text: extractHighlightedText(pageText, quote),
            is_annex: isAnnex,
            annex_name: isAnnex ? docDisplayName : undefined,
          }]
        },
        tags: generateTags(quote)
      });
    }
  }
  
  console.log(`     📊 Layers: certain=${benefits.filter(b => b.layer === 'certain').length}, conditional=${benefits.filter(b => b.layer === 'conditional').length}, service=${benefits.filter(b => b.layer === 'service').length}`);
  return benefits;
}


/**
 * Helper: Generate tags based on benefit content
 */
function generateTags(text) {
  const tags = [];
  const lowerText = text.toLowerCase();
  
  const tagPatterns = {
    'אשפוז': ['אשפוז', 'בית חולים', 'hospitalization'],
    'ניתוח': ['ניתוח', 'surgery', 'operation'],
    'תרופות': ['תרופות', 'medication', 'drugs', 'רוקחות'],
    'שיניים': ['שיניים', 'dental', 'רפואת שיניים'],
    'עיניים': ['עיניים', 'ראייה', 'משקפיים', 'vision', 'optical'],
    'הריון ולידה': ['הריון', 'לידה', 'pregnancy', 'maternity'],
    'בדיקות': ['בדיקות', 'אבחון', 'tests', 'diagnostic'],
    'שיקום': ['שיקום', 'פיזיותרפיה', 'rehabilitation', 'physiotherapy'],
    'נפשי': ['נפש', 'פסיכולוג', 'פסיכיאטר', 'mental', 'psychology'],
    'סיעוד': ['סיעוד', 'סיעודי', 'nursing', 'long-term care'],
  };
  
  for (const [tag, keywords] of Object.entries(tagPatterns)) {
    if (keywords.some(kw => lowerText.includes(kw))) {
      tags.push(tag);
    }
  }
  
  return tags.length > 0 ? tags : null;
}

/**
 * Helper: Calculate layer distribution for metrics
 */
function calculateLayerDistribution(benefits) {
  return {
    certain: benefits.filter(b => b.layer === 'certain').length,
    conditional: benefits.filter(b => b.layer === 'conditional').length,
    service: benefits.filter(b => b.layer === 'service').length
  };
}

/**
 * Helper: Normalize Hebrew text
 */
function normalizeHebrewText(text) {
  return text
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/״/g, '"');
}

/**
 * Helper: Sleep utility
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Start the worker
startWorker().catch(error => {
  console.error('💥 Worker crashed:', error);
  process.exit(1);
});
