export interface IClassifierStats {
    totalClassifications: number;
    cacheHits: number;
    cacheMisses: number;
    vertexAiCalls: number;
    mockClassifications: number;
    csvParseSuccesses: number;
    csvParseFailures: number;
}

export type PersistKind = "record" | "rubbish";
