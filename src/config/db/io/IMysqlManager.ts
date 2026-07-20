export interface ParseJobRow {
    job_id: string;
    batch_id?: string;
    parent_job_id?: string;
    source_type: string;
    source_ref: string;
    s3_url?: string;
    size?: number;
    field_spec: unknown;
    exec_path: string;
    status: string;
    output_paths: unknown;
    counts: Record<string, unknown>;
    timings: Record<string, unknown>;
    error?: string;
    created_at: Date;
    updated_at: Date;
}

export interface OutputPartRow {
    part_id: string;
    job_id: string;
    template_id: string;
    s3_path: string;
    row_count: number;
    byte_size: number;
    created_at: Date;
}

export interface DeadLetterRow {
    dlq_id: string;
    job_id: string;
    byte_offset: number;
    byte_length: number;
    line_no: number;
    raw_bytes: string;
    failure_class: string;
    error: string;
    attempts: number;
    status: string;
    created_at: Date;
    updated_at: Date;
}
