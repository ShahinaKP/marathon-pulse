import { Component, OnDestroy, inject, signal, computed } from '@angular/core';
import { CommonModule, DecimalPipe } from '@angular/common';
import { Subscription } from 'rxjs';
import { UploadService, JobStatusResponse, RowErrorDetail } from '../../services/upload.service';

export interface FileQueueItem {
  id: string; // Internal temporary ID (e.g. file.name + timestamp)
  file: File;
  jobId: string | null;
  status: 'READY' | 'UPLOADING' | 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAILED';
  processedRows: number;
  totalRows: number;
  insertedRows: number;
  duplicateRows: number;
  invalidRows: number;
  progress: number;
  message: string | null;
  errors: RowErrorDetail[];
  subscription?: Subscription;
}

@Component({
  selector: 'app-uploader',
  standalone: true,
  imports: [CommonModule, DecimalPipe],
  templateUrl: './uploader.component.html',
})
export class UploaderComponent implements OnDestroy {
  private readonly uploadService = inject(UploadService);

  // Signals State
  fileQueue = signal<FileQueueItem[]>([]);
  isDragging = signal<boolean>(false);
  globalError = signal<string | null>(null);

  // Computed overview metrics
  totalQueueCount = computed(() => this.fileQueue().length);
  isAnyUploading = computed(() =>
    this.fileQueue().some((i) => i.status === 'UPLOADING' || i.status === 'PROCESSING'),
  );
  completedCount = computed(() => this.fileQueue().filter((i) => i.status === 'COMPLETED').length);
  failedCount = computed(() => this.fileQueue().filter((i) => i.status === 'FAILED').length);

  onDragOver(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.isDragging.set(true);
  }

  onDragLeave(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.isDragging.set(false);
  }

  onDrop(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.isDragging.set(false);

    if (event.dataTransfer?.files && event.dataTransfer.files.length > 0) {
      this.addFilesToQueue(event.dataTransfer.files);
    }
  }

  onFilesSelected(event: Event): void {
    const target = event.target as HTMLInputElement;
    if (target.files && target.files.length > 0) {
      this.addFilesToQueue(target.files);
      target.value = ''; // Reset input to allow re-selection
    }
  }

  private addFilesToQueue(files: FileList): void {
    const newItems: FileQueueItem[] = [];
    const currentQueue = this.fileQueue();
    let invalidCount = 0;
    let legacyXlsCount = 0;
    let duplicateCount = 0;

    Array.from(files).forEach((file) => {
      const fileName = file.name.toLowerCase();

      if (fileName.endsWith('.xls')) {
        // The worker reads the modern .xlsx format only.
        legacyXlsCount++;
        return;
      }
      if (!fileName.endsWith('.xlsx')) {
        invalidCount++;
        return;
      }

      // Same name and size already in the list: skip (this is only a UI convenience;
      // duplicate PEOPLE are detected row by row in the database, not by file).
      const isAlreadyQueued = currentQueue.some(
        (existing) => existing.file.name === file.name && existing.file.size === file.size,
      );

      if (isAlreadyQueued) {
        duplicateCount++;
        return;
      }

      newItems.push({
        id: `${file.name}-${Date.now()}-${Math.random()}`,
        file,
        jobId: null,
        status: 'READY',
        processedRows: 0,
        totalRows: 0,
        insertedRows: 0,
        duplicateRows: 0,
        invalidRows: 0,
        progress: 0,
        message: null,
        errors: [],
      });
    });

    const warnings: string[] = [];
    if (legacyXlsCount > 0) {
      warnings.push(
        `${legacyXlsCount} legacy .xls file(s) excluded - open them in Excel and save as .xlsx.`,
      );
    }
    if (invalidCount > 0) {
      warnings.push(`${invalidCount} non-Excel file(s) excluded.`);
    }
    if (duplicateCount > 0) {
      warnings.push(`${duplicateCount} file(s) already in the list were skipped.`);
    }

    this.globalError.set(warnings.length > 0 ? warnings.join(' ') : null);
    this.fileQueue.update((current) => [...current, ...newItems]);
  }

  startUploadAll(): void {
    const queue = this.fileQueue();
    queue.forEach((item) => {
      if (item.status === 'READY' || item.status === 'FAILED') {
        this.uploadSingleItem(item);
      }
    });
  }

  startUploadSingle(item: FileQueueItem): void {
    if (item.status === 'READY' || item.status === 'FAILED') {
      this.uploadSingleItem(item);
    }
  }

  private uploadSingleItem(item: FileQueueItem): void {
    item.subscription?.unsubscribe();
    this.updateItem(item.id, {
      status: 'UPLOADING',
      message: 'Sending file to the ingestion service...',
      jobId: null,
      processedRows: 0,
      totalRows: 0,
      insertedRows: 0,
      duplicateRows: 0,
      invalidRows: 0,
      progress: 0,
      errors: [],
    });

    this.uploadService.uploadFile(item.file).subscribe({
      next: (res) => {
        this.updateItem(item.id, {
          jobId: res.job_id,
          status: 'QUEUED',
          message: 'Accepted. Waiting for a worker...',
        });
        this.pollItemJob(item.id, res.job_id);
      },
      error: (err) => {
        this.updateItem(item.id, {
          status: 'FAILED',
          message: err.error?.detail || 'Upload failed. Is the API running?',
        });
      },
    });
  }

  private describe(job: JobStatusResponse): string {
    switch (job.status) {
      case 'COMPLETED':
        return (
          `Completed: ${(job.inserted_rows || 0).toLocaleString()} inserted, ` +
          `${(job.duplicate_rows || 0).toLocaleString()} duplicate, ` +
          `${(job.invalid_rows || 0).toLocaleString()} invalid.`
        );
      case 'FAILED':
        return job.error_message || 'Processing failed.';
      case 'PROCESSING':
        return 'Worker is parsing and loading records...';
      default:
        // QUEUED. If an earlier attempt failed, the worker will try again shortly.
        return job.error_message
          ? `Retrying after an error: ${job.error_message}`
          : 'Queued. The cloud worker starts on demand, which can take up to a minute.';
    }
  }

  private pollItemJob(itemId: string, jobId: string): void {
    const sub = this.uploadService.pollJobStatus(jobId).subscribe({
      next: (job: JobStatusResponse) => {
        const total = job.total_rows || 0;
        const processed = job.processed_rows || 0;
        const progress = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;

        this.updateItem(itemId, {
          status: job.status,
          processedRows: processed,
          totalRows: total,
          insertedRows: job.inserted_rows || 0,
          duplicateRows: job.duplicate_rows || 0,
          invalidRows: job.invalid_rows || 0,
          progress: job.status === 'COMPLETED' ? 100 : progress,
          errors: job.errors || [],
          message: this.describe(job),
        });
      },
      error: (err) => {
        this.updateItem(itemId, {
          status: 'FAILED',
          message: err.error?.detail || 'Lost contact with the API while checking status.',
        });
      },
    });

    this.updateItem(itemId, { subscription: sub });
  }

  private updateItem(id: string, patch: Partial<FileQueueItem>): void {
    this.fileQueue.update((items) => items.map((it) => (it.id === id ? { ...it, ...patch } : it)));
  }

  removeItem(item: FileQueueItem): void {
    item.subscription?.unsubscribe();
    this.fileQueue.update((items) => items.filter((it) => it.id !== item.id));
  }

  clearCompleted(): void {
    this.fileQueue.update((items) => items.filter((it) => it.status !== 'COMPLETED'));
  }

  ngOnDestroy(): void {
    this.fileQueue().forEach((item) => item.subscription?.unsubscribe());
  }
}
