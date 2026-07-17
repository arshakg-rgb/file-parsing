import { templateRegistry, RecordTemplate, RubbishTemplate, TemplateKind } from "./templateRegistry.js";
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
  constructor(private registry: typeof templateRegistry) {}

  async classifyLine(
    line: string,
    fieldSpec: string[],
    byteOffset: number,
    lineNo: number
  ): Promise<ClassificationResult> {
    const registryClass = this.registry.constructor as any;
    if (!registryClass.passesLengthGate(line, fieldSpec)) {
      logger.debug("line_dropped_length", { byte_offset: byteOffset, line_no: lineNo });
      return { fate: LineFate.DROPPED_LENGTH, reason: "Failed length gate" };
    }

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

    logger.debug("line_dead_letter", { byte_offset: byteOffset, lineNo: lineNo });
    return { 
      fate: LineFate.DEAD_LETTER, 
      reason: "No template match, requires AI classification" 
    };
  }

  private extractFields(
    line: string, 
    fieldSpec: string[], 
    template: RecordTemplate
  ): Record<string, any> {
    const fields: Record<string, any> = {};
    const parts = line.split(",");
    
    for (let i = 0; i < fieldSpec.length; i++) {
      const fieldName = fieldSpec[i];
      if (i < parts.length) {
        fields[fieldName] = parts[i].trim();
      } else {
        fields[fieldName] = null;
      }
    }
    
    return fields;
  }

  hasMatchRateCollapsed(): boolean {
    return this.registry.hasMatchRateCollapsed();
  }
}
