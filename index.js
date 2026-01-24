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
const POLL_INTERVAL = 2000; // Check queue every 2 seconds
const MAX_RETRIES = 3;

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
      // Poll queue for job (LPOP from Redis list)
      const jobData = await redis.lpop(QUEUE_NAME);

      if (jobData) {
        const job = typeof jobData === 'string' ? JSON.parse(jobData) : jobData;
        console.log(`\n📥 New job received:`, job);

        await processJob(job);
      } else {
        // No jobs, wait before polling again
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
    console.log(`\n🔧 Processing run: ${run_id}`);

    // Update status to running
    await updateRunStatus(run_id, 'running', 'intake');

    // Execute processing pipeline
    await processPolicyPipeline(run_id);

    // Mark as completed
    await updateRunStatus(run_id, 'completed', 'export');

    console.log(`✅ Job completed: ${run_id}`);

  } catch (error) {
    console.error(`❌ Job failed: ${run_id}`, error);

    // Retry logic
    if (retry_count < MAX_RETRIES) {
      console.log(`🔄 Retrying job (attempt ${retry_count + 1}/${MAX_RETRIES})`);
      
      // Re-queue with incremented retry count
      await redis.rpush(QUEUE_NAME, JSON.stringify({
        ...job,
        retry_count: retry_count + 1
      }));
    } else {
      // Max retries reached - mark as failed
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

  // Stage 1: Intake (fetch and extract documents)
  console.log('  1️⃣  Intake - Fetching documents...');
  await updateRunStatus(run_id, 'running', 'intake');
  const documents = await stageIntake(run_id);
  console.log(`     ✓ Extracted ${documents.length} documents`);

  // Stage 2: Map (analyze document structure)
  console.log('  2️⃣  Map - Analyzing structure...');
  await updateRunStatus(run_id, 'running', 'map');
  const structure = await stageMap(run_id, documents);
  console.log(`     ✓ Mapped ${Object.keys(structure).length} sections`);

  // Stage 3: Harvest (extract benefits and evidence)
  console.log('  3️⃣  Harvest - Extracting rights...');
  await updateRunStatus(run_id, 'running', 'harvest');
  const benefits = await stageHarvest(run_id, documents, structure);
  console.log(`     ✓ Found ${benefits.length} benefits`);

  // Stage 4: Normalize (clean and standardize)
  console.log('  4️⃣  Normalize - Standardizing data...');
  await updateRunStatus(run_id, 'running', 'normalize');
  const normalized = await stageNormalize(benefits);
  console.log(`     ✓ Normalized ${normalized.length} benefits`);

  // Stage 5: Validate (check evidence coverage)
  console.log('  5️⃣  Validate - Verifying evidence...');
  await updateRunStatus(run_id, 'running', 'validate');
  const validated = await stageValidate(run_id, normalized);
  console.log(`     ✓ Validated with ${validated.coverage_ratio * 100}% evidence coverage`);

  // Stage 6: Export (generate outputs)
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
  // Fetch documents metadata from Supabase
  const { data: docs, error } = await supabase
    .from('documents')
    .select('*')
    .eq('run_id', run_id);

  if (error) throw error;
  if (!docs || docs.length === 0) {
    throw new Error('No documents found for this run');
  }

  const extractedDocs = [];

  // Extract text from each PDF
  for (const doc of docs) {
    console.log(`     📄 Processing: ${doc.display_name}`);

    // Download PDF from Supabase Storage
    const { data: fileData, error: downloadError } = await supabase
      .storage
      .from('policy-documents')
      .download(doc.storage_key);

    if (downloadError) {
      console.error(`     ⚠️  Failed to download: ${doc.display_name}`);
      continue;
    }

    // Extract text from PDF
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
    // Simple section detection (you'll enhance this later)
    const sections = detectSections(doc.text);
    
    structure[doc.document_id] = {
      doc_type: doc.doc_type,
      sections: sections,
      has_schedule: detectSchedule(doc.text, doc.display_name)
    };
  }

  // Check if schedule is missing
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
    // Find benefit clauses (simplified - you'll enhance with NLP later)
    const foundBenefits = extractBenefits(doc.text, doc.document_id);
    benefits.push(...foundBenefits);
  }

  // Insert benefits into database
  if (benefits.length > 0) {
    const { error } = await supabase
      .from('benefits')
      .insert(benefits.map(b => ({
        run_id,
        ...b
      })));

    if (error) throw error;
  }

  return benefits;
}

/**
 * Stage 4: Normalize - Standardize data
 */
async function stageNormalize(benefits) {
  // Normalize benefit titles, amounts, tags
  return benefits.map(benefit => ({
    ...benefit,
    title: normalizeHebrewText(benefit.title),
    // Add more normalization logic here
  }));
}

/**
 * Stage 5: Validate - Check evidence coverage
 */
async function stageValidate(run_id, benefits) {
  // Check that every benefit has evidence
  const benefitsWithEvidence = benefits.filter(
    b => b.evidence_set && b.evidence_set.spans && b.evidence_set.spans.length > 0
  );

  const coverage_ratio = benefits.length > 0 
    ? benefitsWithEvidence.length / benefits.length 
    : 0;

  // Update quality metrics
  await supabase
    .from('runs')
    .update({
      quality_metrics: {
        evidence_coverage_ratio: coverage_ratio,
        benefits_count: benefits.length,
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
 * Stage 6: Export - Generate output files (placeholder)
 */
async function stageExport(run_id) {
  // TODO: Implement actual export generation
  // For now, just mark as complete
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
  // Simple page splitting (you'll improve this)
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
  // Simple pattern matching for Hebrew section headers
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
 * Helper: Extract benefits from text (simplified)
 */
function extractBenefits(text, document_id) {
  const benefits = [];
  
  // Simple pattern for "entitled to" clauses in Hebrew
  const benefitPattern = /(?:זכאי ל|מכסה|שיפוי|תגמול|החזר)([^.。]+)/g;
  let match;
  
  while ((match = benefitPattern.exec(text)) !== null) {
    const quote = match[0].substring(0, 200); // Max 200 chars
    
    benefits.push({
      benefit_id: randomUUID(),
      layer: 'certain', // Default to certain
      title: quote.substring(0, 100),
      summary: quote,
      status: 'included',
      evidence_set: {
        spans: [{
          document_id,
          page: 1, // TODO: Calculate actual page
          quote,
          verbatim: true,
          confidence: 0.8
        }]
      }
    });
  }
  
  return benefits;
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
