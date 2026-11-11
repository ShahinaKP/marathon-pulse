import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, interval } from 'rxjs';
import { switchMap, takeWhile } from 'rxjs/operators';

export interface RowErrorDetail {
  row: number;
  message: string;
}

export interface JobStatusResponse {
  job_id: string;
  status: 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAILED';
  total_rows: number;
  processed_rows: number;
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
  private readonly baseUrl = 'http://localhost:8000/api';

  uploadFile(file: File): Observable<UploadResponse> {
    const formData = new FormData();
    formData.append('file', file, file.name);
    return this.http.post<UploadResponse>(`${this.baseUrl}/upload`, formData);
  }

  getJobStatus(jobId: string): Observable<JobStatusResponse> {
    return this.http.get<JobStatusResponse>(`${this.baseUrl}/status/${jobId}`);
  }

  pollJobStatus(jobId: string, intervalMs: number = 1000): Observable<JobStatusResponse> {
    return interval(intervalMs).pipe(
      switchMap(() => this.getJobStatus(jobId)),
      takeWhile((job) => job.status === 'QUEUED' || job.status === 'PROCESSING', true),
    );
  }
}
