import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, defer } from 'rxjs';
import { repeat, retry, takeWhile } from 'rxjs/operators';
import { API_BASE_URL } from '../config';

export interface RowErrorDetail {
  row: number;
  message: string;
}

export interface JobStatusResponse {
  job_id: string;
  status: 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAILED';
  total_rows: number;
  processed_rows: number;
  inserted_rows: number;
  duplicate_rows: number;
  invalid_rows: number;
  error_message: string | null;
  created_at: string | null;
  updated_at: string | null;
  errors: RowErrorDetail[];
}

export interface UploadResponse {
  job_id: string;
  status: string;
  message: string;
}

@Injectable({
  providedIn: 'root',
})
export class UploadService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = API_BASE_URL;

  uploadFile(file: File): Observable<UploadResponse> {
    const formData = new FormData();
    formData.append('file', file, file.name);
    return this.http.post<UploadResponse>(`${this.baseUrl}/upload`, formData);
  }

  getJobStatus(jobId: string): Observable<JobStatusResponse> {
    return this.http.get<JobStatusResponse>(`${this.baseUrl}/status/${jobId}`);
  }

  /**
   * Poll until the job reaches COMPLETED or FAILED.
   *
   * The next request is sent only AFTER the previous answer arrives, so a slow response
   * (the database is in the cloud) is never cancelled by the next tick. Brief network
   * errors are retried before giving up.
   */
  pollJobStatus(jobId: string, intervalMs: number = 1000): Observable<JobStatusResponse> {
    return defer(() => this.getJobStatus(jobId)).pipe(
      retry({ count: 3, delay: 2000 }),
      repeat({ delay: intervalMs }),
      takeWhile((job) => job.status === 'QUEUED' || job.status === 'PROCESSING', true),
    );
  }
}
