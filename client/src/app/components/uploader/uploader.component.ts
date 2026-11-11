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
    const validExtensions = ['.xlsx', '.xls'];
    const newItems: FileQueueItem[] = [];
    let invalidCount = 0;

    Array.from(files).forEach((file) => {
      const fileName = file.name.toLowerCase();
      const isValid = validExtensions.some((ext) => fileName.endsWith(ext));

      if (isValid) {
        newItems.push({
          id: `${file.name}-${Date.now()}-${Math.random()}`,
          file,
          jobId: null,
          status: 'READY',
          processedRows: 0,
          totalRows: 0,
          progress: 0,
          message: null,
          errors: [],
        });
      } else {
        invalidCount++;
      }
    });

    if (invalidCount > 0) {
      this.globalError.set(
        `${invalidCount} non-Excel file(s) were excluded. Only .xlsx and .xls are supported.`,
      );
    } else {
      this.globalError.set(null);
    }

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
    this.updateItem(item.id, {
      status: 'UPLOADING',
      message: 'Sending payload to ingestion service...',
    });

    this.uploadService.uploadFile(item.file).subscribe({
      next: (res) => {
        this.updateItem(item.id, {
          jobId: res.job_id,
          status: (res.status as FileQueueItem['status']) || 'QUEUED',
          message: res.message,
        });
        this.pollItemJob(item.id, res.job_id);
      },
      error: (err) => {
        this.updateItem(item.id, {
          status: 'FAILED',
          message: err.error?.detail || 'Upload failed.',
        });
      },
    });
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
          progress: job.status === 'COMPLETED' ? 100 : progress,
          errors: job.errors || [],
          message:
            job.status === 'COMPLETED'
              ? 'Ingestion completed successfully'
              : job.status === 'PROCESSING'
                ? 'Parsing records in background...'
                : 'Queued in buffer...',
        });
      },
      error: (err) => {
        this.updateItem(itemId, {
          status: 'FAILED',
          message: err.error?.detail || 'Error querying status.',
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
