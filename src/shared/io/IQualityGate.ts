export interface QualityMetrics {
    totalLines: number;
    parsedLines: number;
    droppedRubbishLines: number;
    failedLines: number;
    failedLineRatio: number;
}
