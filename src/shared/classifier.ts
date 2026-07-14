import { TemplateRegistry, RecordTemplate, RubbishTemplate, TemplateKind } from "./templateRegistry.js";
import { createLogger } from "./logger.js";

const logger = createLogger("classifier");

export enum LineFate {
  DROPPED_LENGTH = "dropped_length",
  PARSED = "parsed",
  DROPPED_RUBBISH = "dropped_rubbish",
  DEAD_LETTER = "dead_letter",
}

export interface ClassificationResult {
  fate: LineFate;
  template?: RecordTemplate | RubbishTemplate;
  extractedFields?: Record<string, any>;
  reason?: string;
}

export class LineClassifier {
  constructor(private registry: TemplateRegistry) {}

  /**
   * Main classification method implementing the ordered classifier
   * Order: Length gate → Record templates → Rubbish templates → AI
   */
  async classifyLine(
    line: string,
    fieldSpec: string[],
    byteOffset: number,
    lineNo: number
  ): Promise<ClassificationResult> {
    // Step 1: Length gate
    if (!TemplateRegistry.passesLengthGate(line, fieldSpec)) {
      logger.debug("line_dropped_length", { byte_offset: byteOffset, line_no: lineNo });
      return { fate: LineFate.DROPPED_LENGTH, reason: "Failed length gate" };
    }

    // Step 2: Record templates
    const recordTemplate = this.registry.matchRecordTemplate(line, fieldSpec);
    if (recordTemplate) {
      const extractedFields = this.extractFields(line, fieldSpec, recordTemplate);
      logger.debug("line_parsed_template", { 
        template_id: recordTemplate.template_id, 
        byte_offset: byteOffset, 
        line_no: lineNo 
      });
      return { 
        fate: LineFate.PARSED, 
        template: recordTemplate, 
        extractedFields 
      };
    }

    // Step 3: Rubbish templates (high confidence only)
    const rubbishTemplate = this.registry.matchRubbishTemplate(line);
    if (rubbishTemplate) {
      logger.debug("line_dropped_rubbish", { 
        template_id: rubbishTemplate.template_id, 
        byte_offset: byteOffset, 
        line_no: lineNo 
      });
      return { 
        fate: LineFate.DROPPED_RUBBISH, 
        template: rubbishTemplate, 
        reason: "Matched rubbish template" 
      };
    }

    // Step 4: AI (only for unknown lines)
    // This would call the AI Classifier service
    // For now, return dead letter as AI integration is pending
    logger.debug("line_dead_letter", { byte_offset: byteOffset, line_no: lineNo });
    return { 
      fate: LineFate.DEAD_LETTER, 
      reason: "No template match, requires AI classification" 
    };
  }

  /**
   * Extract fields from line using template field map
   */
  private extractFields(
    line: string, 
    fieldSpec: string[], 
    template: RecordTemplate
  ): Record<string, any> {
    const fields: Record<string, any> = {};
    
    // Simple CSV extraction based on field positions
    // In production, this would use the template's field_map to locate fields
    const parts = line.split(",");
    
    for (let i = 0; i < fieldSpec.length; i++) {
      const fieldName = fieldSpec[i];
      if (i < parts.length) {
        fields[fieldName] = parts[i].trim();
      } else {
        fields[fieldName] = null; // Missing field
      }
    }
    
    return fields;
  }

  /**
   * Check if match rate has collapsed (AI cost bounding)
   */
  hasMatchRateCollapsed(): boolean {
    return this.registry.hasMatchRateCollapsed();
  }
}
