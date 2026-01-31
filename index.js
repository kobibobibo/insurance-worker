/**
 * Helper: Classify benefit layer based on text content
 */
function classifyBenefitLayer(text) {
  const lowerText = text.toLowerCase();
  
  // Conditional indicators (requires conditions/approvals)
  const conditionalKeywords = [
    'בכפוף ל', 'באישור', 'לאחר תקופת המתנה', 'בתנאי ש',
    'subject to', 'pending approval', 'waiting period',
    'מותנה', 'תקופת אכשרה', 'לאחר אישור'
  ];
  
  // Service indicators (assistance/support services)
  const serviceKeywords = [
    'שירות', 'סיוע', 'ייעוץ', 'מוקד', 'תמיכה',
    'service', 'assistance', 'helpline', 'support',
    'הנחה', 'הטבה', 'שירותי רווחה'
  ];
  
  if (conditionalKeywords.some(kw => lowerText.includes(kw))) {
    return 'conditional';
  }
  
  if (serviceKeywords.some(kw => lowerText.includes(kw))) {
    return 'service';
  }
  
  return 'certain';
}

/**
 * Helper: Extract benefits from text
 */
function extractBenefits(text, document_id) {
  const benefits = [];
  
  const benefitPattern = /(?:זכאי ל|מכסה|שיפוי|תגמול|החזר)([^.。]+)/g;
  let match;
  
  while ((match = benefitPattern.exec(text)) !== null) {
    const quote = match[0].substring(0, 200);
    
    benefits.push({
      benefit_id: randomUUID(),
      layer: classifyBenefitLayer(quote), // ← DYNAMIC NOW
      title: quote.substring(0, 100),
      summary: quote,
      status: 'included',
      evidence_set: {
        spans: [{
          document_id,
          page: 1,
          quote,
          verbatim: true,
          confidence: 0.8
        }]
      }
    });
  }
  
  return benefits;
}
