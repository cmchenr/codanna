use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::BatchingConfig;
use crate::FileId;

/// Tracks system memory information
pub struct MemoryMonitor {
    last_check: AtomicU64,
    available_mb: AtomicU64,
    total_mb: AtomicU64,
}

impl MemoryMonitor {
    pub fn new() -> Self {
        Self {
            last_check: AtomicU64::new(0),
            available_mb: AtomicU64::new(0),
            total_mb: AtomicU64::new(0),
        }
    }

    /// Get current available memory in MB (cached for 1 second)
    pub fn available_memory_mb(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last_check = self.last_check.load(Ordering::Relaxed);
        
        // Update memory info at most once per second
        if now > last_check {
            if let Ok(mem_info) = sys_info::mem_info() {
                let available = (mem_info.avail * 1024) / (1024 * 1024);
                let total = (mem_info.total * 1024) / (1024 * 1024);
                
                self.available_mb.store(available, Ordering::Relaxed);
                self.total_mb.store(total, Ordering::Relaxed);
                self.last_check.store(now, Ordering::Relaxed);
                
                available
            } else {
                // Fallback if sys_info fails
                self.available_mb.load(Ordering::Relaxed)
            }
        } else {
            self.available_mb.load(Ordering::Relaxed)
        }
    }

    /// Get total system memory in MB
    pub fn total_memory_mb(&self) -> u64 {
        self.total_mb.load(Ordering::Relaxed)
    }

    /// Check if memory is critically low
    pub fn is_memory_critical(&self, threshold_mb: u32) -> bool {
        self.available_memory_mb() < threshold_mb as u64
    }
}

/// Adaptive batch manager that optimizes batch sizes based on system conditions
pub struct AdaptiveBatchManager {
    config: Arc<BatchingConfig>,
    memory_monitor: Arc<MemoryMonitor>,
    
    // Current batch state
    current_files: Vec<FileId>,
    estimated_memory_mb: u64,
    batch_start_time: Instant,
    
    // Performance tracking
    recent_batch_times: VecDeque<Duration>,
    recent_throughputs: VecDeque<f32>, // files per second
    
    // Adaptive parameters
    current_batch_size: usize,
    is_watch_mode: bool,
}

impl AdaptiveBatchManager {
    pub fn new(config: Arc<BatchingConfig>, is_watch_mode: bool) -> Self {
        let memory_monitor = Arc::new(MemoryMonitor::new());
        
        // Initialize memory info
        memory_monitor.available_memory_mb();
        
        let initial_batch_size = if is_watch_mode {
            config.watch_batch_size
        } else if config.adaptive_sizing {
            Self::calculate_adaptive_batch_size(&config, &memory_monitor)
        } else {
            config.initial_batch_size
        };

        Self {
            config,
            memory_monitor,
            current_files: Vec::new(),
            estimated_memory_mb: 0,
            batch_start_time: Instant::now(),
            recent_batch_times: VecDeque::with_capacity(10),
            recent_throughputs: VecDeque::with_capacity(10),
            current_batch_size: initial_batch_size,
            is_watch_mode,
        }
    }

    /// Calculate adaptive batch size based on available memory
    fn calculate_adaptive_batch_size(config: &BatchingConfig, monitor: &MemoryMonitor) -> usize {
        let available_mb = monitor.available_memory_mb();
        let _total_mb = monitor.total_memory_mb();
        
        // Base batch size on available memory
        match available_mb {
            0..=1000 => config.initial_batch_size.min(25),        // < 1GB: very small batches
            1001..=2000 => config.initial_batch_size.min(50),     // 1-2GB: small batches  
            2001..=4000 => config.initial_batch_size,             // 2-4GB: default
            4001..=8000 => config.initial_batch_size * 2,         // 4-8GB: larger batches
            _ => config.initial_batch_size * 3,                   // >8GB: large batches
        }.max(1) // Always at least 1
    }

    /// Add a file to the current batch
    pub fn add_file(&mut self, file_id: FileId, estimated_size_mb: f32) {
        self.current_files.push(file_id);
        self.estimated_memory_mb += estimated_size_mb.ceil() as u64;
    }

    /// Check if the batch should be committed
    pub fn should_commit(&mut self, is_last_file: bool) -> bool {
        // Always commit on last file if batch is not empty
        if is_last_file && !self.current_files.is_empty() {
            return true;
        }

        // Watch mode: immediate commits (batch size typically 1)
        if self.is_watch_mode && self.current_files.len() >= self.current_batch_size {
            return true;
        }

        // Emergency commit on critically low memory
        if self.memory_monitor.is_memory_critical(self.config.emergency_memory_mb) {
            return !self.current_files.is_empty();
        }

        // Commit if estimated batch memory exceeds limit
        if self.estimated_memory_mb >= self.config.max_batch_memory_mb as u64 {
            return true;
        }

        // Commit if batch size reached
        if self.current_files.len() >= self.current_batch_size {
            return true;
        }

        // Time-based commit
        if self.config.commit_interval_secs > 0 {
            let elapsed = self.batch_start_time.elapsed().as_secs();
            if elapsed >= self.config.commit_interval_secs as u64 && !self.current_files.is_empty() {
                return true;
            }
        }

        false
    }

    /// Complete a batch and prepare for the next one
    pub fn complete_batch(&mut self, actual_processing_time: Duration) -> Vec<FileId> {
        let files = std::mem::take(&mut self.current_files);
        let files_processed = files.len();
        
        // Record performance metrics
        self.recent_batch_times.push_back(actual_processing_time);
        if self.recent_batch_times.len() > 10 {
            self.recent_batch_times.pop_front();
        }
        
        if files_processed > 0 && actual_processing_time.as_secs_f32() > 0.0 {
            let throughput = files_processed as f32 / actual_processing_time.as_secs_f32();
            self.recent_throughputs.push_back(throughput);
            if self.recent_throughputs.len() > 10 {
                self.recent_throughputs.pop_front();
            }
        }

        // Adapt batch size for next batch (only in initial indexing mode)
        if !self.is_watch_mode && self.config.adaptive_sizing {
            self.adapt_batch_size();
        }

        // Reset for next batch
        self.estimated_memory_mb = 0;
        self.batch_start_time = Instant::now();

        files
    }

    /// Adapt batch size based on recent performance
    fn adapt_batch_size(&mut self) {
        if self.recent_throughputs.len() < 3 {
            return; // Need more data
        }

        let avg_throughput: f32 = self.recent_throughputs.iter().sum::<f32>() / self.recent_throughputs.len() as f32;
        let available_mb = self.memory_monitor.available_memory_mb();

        // Increase batch size if:
        // - Throughput is good (>10 files/sec)
        // - Memory is plentiful (>2GB available)
        // - Current batch size is below max
        if avg_throughput > 10.0 
            && available_mb > 2000 
            && self.current_batch_size < self.config.initial_batch_size * 3 {
            self.current_batch_size = (self.current_batch_size * 110 / 100).max(self.current_batch_size + 1);
        }
        // Decrease batch size if:
        // - Throughput is poor (<5 files/sec)  
        // - Memory is getting low (<1GB available)
        else if avg_throughput < 5.0 
            || available_mb < 1000 {
            self.current_batch_size = (self.current_batch_size * 90 / 100).max(1);
        }
    }

    /// Get current batch statistics
    pub fn get_stats(&self) -> BatchStats {
        BatchStats {
            files_in_batch: self.current_files.len(),
            estimated_memory_mb: self.estimated_memory_mb,
            current_batch_size: self.current_batch_size,
            available_memory_mb: self.memory_monitor.available_memory_mb(),
            avg_throughput: self.recent_throughputs.iter().sum::<f32>() / self.recent_throughputs.len().max(1) as f32,
        }
    }

    /// Check if memory is currently under pressure
    pub fn is_memory_pressure(&self) -> bool {
        self.memory_monitor.is_memory_critical(self.config.emergency_memory_mb)
    }
}

/// Batch processing statistics
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub files_in_batch: usize,
    pub estimated_memory_mb: u64,
    pub current_batch_size: usize,
    pub available_memory_mb: u64,
    pub avg_throughput: f32,
}