;;; org-duckdb-blocks.el --- Track DuckDB block executions in Org files -*- lexical-binding: t; -*-

;; Author: gggion
;; Version: 1.0.0
;; Package-Requires: ((emacs "28.1") (org "9.5"))
;; Keywords: org, duckdb, data
;; URL: https://github.com/gggion/ob-duckdb

;;; Commentary:

;; The `org-duckdb-blocks' package provides a comprehensive tracking and
;; management system for DuckDB source blocks in Org mode documents.  It serves
;; as the foundation for execution history, block identification, and navigation
;; functionality in the `ob-duckdb' ecosystem.
;;
;; Key Features:
;;
;; - Persistent Block Identity: Assigns and maintains unique IDs for DuckDB
;;   source blocks across editing sessions using buffer-local properties
;;
;; - Execution History: Records each execution with timestamps, content
;;   snapshots, and parameter information
;;
;; - Position Tracking: Maintains up-to-date coordinates of blocks even as
;;   document content changes
;;
;; - Navigation Tools: Provides commands to revisit any previously executed
;;   block
;;
;; - State Preservation: Captures header arguments, switches, and content at
;;   execution time
;;
;; - Automatic Cleanup: Removes stale entries when blocks are deleted or moved
;;
;; This system was primarily designed to support asynchronous execution in
;; `ob-duckdb-async', allowing results to be correctly routed back to their
;; source blocks even after substantial document edits. However, it also
;; provides general-purpose execution tracking and navigation that's useful
;; independently.
;;
;; To use this package, simply require it from `ob-duckdb' or activate it
;; directly with:
;;
;;   (require 'org-duckdb-blocks)
;;   (org-duckdb-blocks-setup)
;;
;; Internally, the package maintains two primary data structures:
;;
;; 1. A registry mapping block IDs to their current positions and content
;; 2. An execution history mapping execution IDs to execution details
;;
;; Additionally, a circular buffer tracks recent executions for fast access and
;; navigation.
;;
;; The block tracking occurs automatically via advice on `org-babel-execute-src-block',
;; capturing information whenever a DuckDB block is executed.

;;; Code:

(require 'org-element)
(require 'org-macs)
(require 'org-id)

;; --- ID normalization utility -----------------------------
(defun org-duckdb-blocks-normalize-id (id)
  "Return ID as plain string without any text properties.
This is critical for consistent hashtable lookups since text properties
can cause string equality comparisons to fail even when the visible
content is identical. All IDs in the system should be normalized
with this function before storage or comparison."
  (if (stringp id) (substring-no-properties id) id))
;; ----------------------------------------------------------

;;; Core Data Structures

(defvar org-duckdb-blocks-registry (make-hash-table :test 'equal)
  "Registry mapping block IDs to metadata.
The registry is the primary data structure tracking all known DuckDB blocks.
Each entry maps a block ID (string) to a property list with:

`:begin'     - Position of block beginning
`:end'       - Position of block end
`:file'      - File containing the block
`:buffer'    - Buffer name containing the block
`:content'   - Current block content (SQL text)")

(defvar org-duckdb-blocks-executions (make-hash-table :test 'equal)
  "Execution history mapping execution IDs to details.
Maintains the complete history of all DuckDB block executions.
Each entry maps an execution ID (string) to a property list with:

`:block-id'    - ID of the source block executed
`:begin'       - Block position at execution time
`:time'        - Timestamp of execution
`:content'     - Content executed (snapshot)
`:parameters'  - Header parameters used
`:header'      - Parsed header arguments
`:switches'    - Block switches applied
`:line-count'  - Number of lines in content
`:name'        - Name of the block (if any)")

(defvar org-duckdb-blocks-history-vector (make-vector 100 nil)
  "Circular buffer containing recent executions for fast access.
This data structure provides O(1) access to the most recent executions
without having to scan the full executions hash table. Each slot
contains a vector with [exec-id block-id timestamp] for quick rendering
of history lists and navigation controls.")

(defvar org-duckdb-blocks-history-index 0
  "Current position in the history vector's circular buffer.
Indicates where the next execution record will be written in
`org-duckdb-blocks-history-vector'.")

(defvar org-duckdb-blocks-history-count 0
  "Number of entries currently stored in the history vector.
Used to determine how many valid entries exist in the circular
buffer when retrieving recent history, especially before the buffer
has been completely filled.")

(defvar org-duckdb-blocks-history-capacity 100
  "Maximum number of recent executions to store in the history vector.
This limits the size of the circular buffer to prevent unbounded growth
while still providing convenient access to recent execution history.")

;;; Core Utility Functions

(defun org-duckdb-blocks-add-to-history (exec-id block-id timestamp)
  "Record execution in history buffer for EXEC-ID, BLOCK-ID at TIMESTAMP.
Adds an entry to the circular history buffer, ensuring IDs are
normalized and overwriting the oldest entry when the buffer is full.
This maintains a fixed-size record of the most recent executions."
  (let ((entry (vector (org-duckdb-blocks-normalize-id exec-id) (org-duckdb-blocks-normalize-id block-id) timestamp)))
    ;; Store at current index and advance
    (aset org-duckdb-blocks-history-vector org-duckdb-blocks-history-index entry)
    ;; Update index with wraparound and count with capping
    (setq org-duckdb-blocks-history-index
          (% (1+ org-duckdb-blocks-history-index) org-duckdb-blocks-history-capacity)
          org-duckdb-blocks-history-count
          (min (1+ org-duckdb-blocks-history-count) org-duckdb-blocks-history-capacity))))

(defun org-duckdb-blocks-get-recent-history (n)
  "Retrieve the N most recent executions from history, newest first.
Returns a list of execution entries from the circular history buffer.
Each entry is a vector of [exec-id block-id timestamp].
This enables efficient access to recent history without scanning
the full executions hash table."
  (let* ((count (min n org-duckdb-blocks-history-count))
         ;; Calculate starting index based on whether buffer is full
         (start-idx (if (< org-duckdb-blocks-history-count org-duckdb-blocks-history-capacity)
                        ;; Not full yet, start from the highest filled index
                        (1- org-duckdb-blocks-history-count)
                      ;; Full buffer, start from the position before the current write position
                      (mod (+ org-duckdb-blocks-history-index
                             (- org-duckdb-blocks-history-capacity 1))
                           org-duckdb-blocks-history-capacity)))
         result)
    ;; Collect entries working backward from start-idx
    (dotimes (i count result)
      (let* ((idx (mod (- start-idx i) org-duckdb-blocks-history-capacity))
             (entry (aref org-duckdb-blocks-history-vector idx)))
        (when entry (push entry result))))))

;;; Property Management

(defun org-duckdb-blocks-update-properties (block-id exec-id begin)
  "Update properties for block with BLOCK-ID and EXEC_ID at BEGIN.
Inserts or updates #+PROPERTY: lines before the source block to maintain
persistent identification across editing sessions. These properties are
used to recover block identity after reopening files and to identify
the most recent execution of each block.

Both IDs are normalized to prevent issues with text properties affecting
string comparison operations. The properties are inserted directly before
the source block, searching back a limited distance to find existing ones."
  (let ((block-id (org-duckdb-blocks-normalize-id block-id))
        (exec-id  (org-duckdb-blocks-normalize-id exec-id)))
    (save-excursion
      (goto-char begin)

      ;; Update or add ID property
      (if (re-search-backward "^#\\+PROPERTY: ID " (max (- begin 200) (point-min)) t)
          (progn
            (beginning-of-line)
            (delete-region (point) (line-end-position))
            (insert (format "#+PROPERTY: ID %s" block-id)))
        (goto-char begin)
        (forward-line -1)
        (end-of-line)
        (insert (format "\n#+PROPERTY: ID %s" block-id)))

      ;; Update or add EXEC_ID property
      (goto-char begin)
      (if (re-search-backward "^#\\+PROPERTY: EXEC_ID " (max (- begin 200) (point-min)) t)
          (progn
            (beginning-of-line)
            (delete-region (point) (line-end-position))
            (insert (format "#+PROPERTY: EXEC_ID %s" exec-id)))
        (goto-char begin)
        (forward-line -1)
        (end-of-line)
        (insert (format "\n#+PROPERTY: EXEC_ID %s" exec-id))))))

;;; Block Registration and Management

(defun org-duckdb-blocks-get-block-id (begin)
  "Get block ID from properties for block at BEGIN, or nil if not found.
Searches backward from BEGIN to find an ID property associated with the block.
Returns a normalized (property-stripped) ID string if found, or nil otherwise.
The search is limited to a reasonable distance to avoid matching properties
from other blocks."
  (save-excursion
    (goto-char begin)
    (when (re-search-backward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)" (max (- begin 200) (point-min)) t)
      (org-duckdb-blocks-normalize-id (match-string 1)))))

(defun org-duckdb-blocks-update-all-block-positions ()
  "Update positions of all tracked blocks and clean up stale entries.
This comprehensive function synchronizes the registry with the
current buffer state:

1. Collects all blocks in current buffer with their IDs
2. Updates coordinates for blocks that still have IDs
3. Removes blocks from registry that are no longer in the buffer
4. Identifies and removes duplicate block entries for the same position

This ensures the registry stays accurate as blocks are added, removed,
or edited. It's called automatically before registering executions
to maintain consistency."
  (let ((file (buffer-file-name))
        (buffer (buffer-name))
        ;; Map positions to block IDs to detect duplicates
        (found-blocks (make-hash-table :test 'equal))
        ;; List of all block IDs found in this buffer
        (current-buffer-blocks nil))

    ;; First pass: find blocks with IDs in the current buffer
    (save-excursion
      (goto-char (point-min))
      (while (re-search-forward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)" nil t)
        (let ((block-id (org-duckdb-blocks-normalize-id (match-string 1))))
          (push block-id current-buffer-blocks)

          ;; Find the associated source block
          (when (re-search-forward "^#\\+begin_src duckdb" nil t)
            (let* ((element (org-element-at-point))
                   (begin (org-element-property :begin element))
                   (end (org-element-property :end element)))

              ;; Store position -> ID mapping
              (when (and begin end)
                (puthash (cons begin end) block-id found-blocks)

                ;; Update block coordinates in registry
                (let* ((contents-begin (org-element-property :contents-begin element))
                       (contents-end (org-element-property :contents-end element))
                       (content (if (and contents-begin contents-end)
                                    (buffer-substring-no-properties contents-begin contents-end)
                                  ""))
                       (info (gethash block-id org-duckdb-blocks-registry)))
                  (if info
                      (puthash block-id
                               (list :begin begin
                                     :end end
                                     :file file
                                     :buffer buffer
                                     :content content)
                               org-duckdb-blocks-registry)
                    (puthash block-id
                             (list :begin begin
                                   :end end
                                   :file file
                                   :buffer buffer
                                   :content content)
                             org-duckdb-blocks-registry))))))))

    ;; Second pass: find blocks in registry that should be deleted
    (let ((blocks-to-remove nil))
      ;; Check each block in registry to see if it should be removed
      (maphash (lambda (block-id info)
                 (when (and (equal (plist-get info :buffer) buffer)
                            (equal (plist-get info :file) file))
                   (let* ((begin (plist-get info :begin))
                          (end (plist-get info :end))
                          (pos-key (cons begin end))
                          (current-id (gethash pos-key found-blocks)))
                     ;; Both current-id and block-id are plain
                     (when (or (not (member block-id current-buffer-blocks))
                               (and current-id (not (equal current-id block-id))))
                       (push block-id blocks-to-remove)))))
               org-duckdb-blocks-registry)

      ;; Remove blocks that should be deleted
      (when blocks-to-remove
        (dolist (block-id blocks-to-remove)
          (message "[duckdb-blocks] Removing stale block %s from registry"
                   (substring block-id 0 8))
          (remhash block-id org-duckdb-blocks-registry)))

    ;; Return the list of blocks currently in buffer
    current-buffer-blocks))))

(defun org-duckdb-blocks-register-execution ()
  "Register execution of the DuckDB block at point.
This is the core function of the package, capturing the state of a
DuckDB source block at the moment of execution. It:

1. Identifies the source block and extracts its properties
2. Updates the registry with current block positions
3. Assigns a block ID (reusing existing one if available)
4. Creates a new execution ID
5. Records execution details and block content
6. Updates the history buffer
7. Inserts property markers in the document

Returns a plist with :block-id and :exec-id for other components
to reference. This function is automatically called via advice
before executing DuckDB source blocks."
  (interactive)
  (let* ((el (org-element-at-point))
         (is-duckdb (and (eq (car el) 'src-block)
                         (string= (org-element-property :language el) "duckdb"))))
    (when is-duckdb
      ;; --- PREP: Read current block properties ---
      (let* ((begin (org-element-property :begin el))
             (end (org-element-property :end el))
             (contents-begin (org-element-property :contents-begin el))
             (contents-end (org-element-property :contents-end el))
             (content (or (org-element-property :value el)
                          (and contents-begin contents-end
                               (buffer-substring-no-properties contents-begin contents-end))
                          ""))
             (file (buffer-file-name))
             (buffer (buffer-name))
             (parameters (org-element-property :parameters el))
             (header (org-element-property :header el))
             (switches (org-element-property :switches el))
             (line-count (with-temp-buffer
                           (insert content)
                           (count-lines (point-min) (point-max))))
             (name (org-element-property :name el)))

        ;; --- 1. Find existing ID (either from property or by position) ---
        (let* ((existing-by-position nil)
               (existing-id nil))
          (maphash (lambda (id info)
                     (when (and (not existing-by-position)
                                (equal (plist-get info :begin) begin)
                                (equal (plist-get info :end) end)
                                (equal (plist-get info :buffer) buffer)
                                (equal (plist-get info :file) file))
                       (setq existing-by-position id)))
                   org-duckdb-blocks-registry)

          (setq existing-id (or (org-duckdb-blocks-get-block-id begin)
                                existing-by-position))

          ;; --- 2. Finalize IDs ---
          (let* ((block-id (org-duckdb-blocks-normalize-id (or existing-id (org-id-uuid))))
                 (exec-id  (org-duckdb-blocks-normalize-id (org-id-uuid)))
                 (timestamp (current-time)))
            ;; --- 3. Insert/Update property lines (may move the block!) ---
            (org-duckdb-blocks-update-properties block-id exec-id begin)

            ;; --- 4. FULLY RESCAN block positions in buffer ---
            (org-duckdb-blocks-update-all-block-positions)

            ;; --- 5. Lookup latest block info (positions are now correct) ---
            (let ((block-info (gethash block-id org-duckdb-blocks-registry)))
              ;; Defensive: if block-info not found, fallback to old values
              (let ((new-begin (or (plist-get block-info :begin) begin))
                    (new-end   (or (plist-get block-info :end) end))
                    (new-content (or (plist-get block-info :content) content)))
                ;; --- 6. Store registry and history with correct positions ---
                (puthash block-id
                         (list :begin new-begin
                               :end new-end
                               :file file
                               :buffer buffer
                               :content new-content)
                         org-duckdb-blocks-registry)

                (puthash exec-id
                         (list :block-id block-id
                               :begin new-begin
                               :time timestamp
                               :content new-content
                               :parameters parameters
                               :header header
                               :switches switches
                               :line-count line-count
                               :name name)
                         org-duckdb-blocks-executions)

                (org-duckdb-blocks-add-to-history exec-id block-id timestamp)

                (message "[duckdb-blocks] Registered block %s execution %s"
                         (substring block-id 0 8)
                         (substring exec-id 0 8))
                (list :block-id block-id :exec-id exec-id)))))))))

(defun org-duckdb-blocks-goto-block (id)
  "Navigate to the DuckDB source block with ID.
Finds and jumps to a source block in the registry by its ID.
If the block is in another file, that file is opened.
The cursor is positioned at the beginning of the block.

When called interactively, provides completion for all known block IDs."
  (interactive
   (list (let ((choice (completing-read "Block ID: " (hash-table-keys org-duckdb-blocks-registry))))
           (org-duckdb-blocks-normalize-id choice))))
  (when-let* ((info (gethash (org-duckdb-blocks-normalize-id id) org-duckdb-blocks-registry)))
    ;; Navigate to file/buffer
    (cond
     ((and (plist-get info :file) (file-exists-p (plist-get info :file)))
      (find-file (plist-get info :file)))
     ((and (plist-get info :buffer) (get-buffer (plist-get info :buffer)))
      (switch-to-buffer (plist-get info :buffer)))
     (t (message "[duckdb-blocks] Source not found")))

    ;; Position cursor
    (goto-char (plist-get info :begin))
    (recenter-top-bottom)))

(defun org-duckdb-blocks-goto-execution (exec-id)
  "Navigate to source block of execution EXEC-ID.
Finds a specific execution in the history and jumps to its
associated source block. This allows revisiting blocks based on
their execution history rather than just their block ID.

When called interactively, provides completion for all executions."
  (interactive
   (list (let ((choice (completing-read "Execution ID: " (hash-table-keys org-duckdb-blocks-executions))))
           (org-duckdb-blocks-normalize-id choice))))
  (when-let* ((exec-info (gethash (org-duckdb-blocks-normalize-id exec-id) org-duckdb-blocks-executions))
              (block-id (plist-get exec-info :block-id)))
    (org-duckdb-blocks-goto-block block-id)))

(defun org-duckdb-blocks-execution-info (exec-id)
  "Display detailed information about the execution with EXEC-ID.
Shows a comprehensive report of an execution in a help buffer, including:
- Block identification and location
- Execution timestamp
- Source block parameters and state at execution time
- Content executed

This is useful for debugging and understanding past executions.
When called interactively, provides completion for all executions."
  (interactive
   (list (let ((choice (completing-read "Execution ID: " (hash-table-keys org-duckdb-blocks-executions))))
           (org-duckdb-blocks-normalize-id choice))))
  (when-let* ((exec-info (gethash (org-duckdb-blocks-normalize-id exec-id) org-duckdb-blocks-executions))
              (block-id (plist-get exec-info :block-id))
              (block-info (gethash (org-duckdb-blocks-normalize-id block-id) org-duckdb-blocks-registry)))
    (with-help-window "*DuckDB Execution Info*"
      (princ (format "DuckDB Execution: %s\n" exec-id))
      (princ "==========================\n\n")

      ;; Block information
      (princ (format "Block ID: %s\n" block-id))
      (princ (format "File: %s\n" (or (plist-get block-info :file) "N/A")))
      (princ (format "Buffer: %s\n" (or (plist-get block-info :buffer) "N/A")))
      (when-let ((name (plist-get exec-info :name)))
        (princ (format "Named Block: %s\n" name)))

      ;; Timestamp
      (princ (format "\nExecution Time: %s\n\n"
                     (format-time-string "%Y-%m-%d %H:%M:%S.%3N"
                                        (plist-get exec-info :time))))

      ;; Source block state
      (princ "=== SOURCE BLOCK STATE ===\n")
      (when-let ((params (plist-get exec-info :parameters)))
        (princ (format "Parameters: %s\n" params)))
      (when-let ((header (plist-get exec-info :header)))
        (princ (format "Header Arguments: %S\n" header)))
      (when-let ((switches (plist-get exec-info :switches)))
        (princ (format "Switches: %s\n" switches)))
      (princ (format "Line Count: %d\n\n"
                    (or (plist-get exec-info :line-count) 0)))

      ;; Get content from execution info
      (let ((content (plist-get exec-info :content)))
        (princ "=== CONTENT ===\n")
        (if (and content (not (string-empty-p content)))
            (princ content)
          (princ "[No content available]"))
        (princ "\n")))))

(defun org-duckdb-blocks-navigate-recent ()
  "Navigate through recent executions with completion.
Presents an interactive list of recent executions with readable
labels and allows jumping to any of them. This is the most convenient
way to revisit recently executed blocks.

Labels include:
- Timestamp of execution
- Block name or ID
- Parameters used (if any)"
  (interactive)
  (let* ((recent (org-duckdb-blocks-get-recent-history 30))
         (options '())
         (selected nil))

    ;; Build options with readable labels
    (dolist (entry recent)
      (let* ((exec-id (aref entry 0))
             (block-id (aref entry 1))
             (timestamp (aref entry 2))
             (exec-info (gethash (org-duckdb-blocks-normalize-id exec-id) org-duckdb-blocks-executions))
             (time-str (format-time-string "%Y-%m-%d %H:%M:%S" timestamp))
             (params (plist-get exec-info :parameters))
             (label (format "[%s] %s %s"
                            time-str
                            (if (plist-get exec-info :name)
                                (format "%s" (plist-get exec-info :name))
                              (format "Block %s" (substring (org-duckdb-blocks-normalize-id block-id) 0 8)))
                            (if params (format " (%s)" params) ""))))
        (push (cons label exec-id) options)))

    (when options
      (setq selected (cdr (assoc (completing-read "Go to: " options nil t) options)))
      (when selected
        (org-duckdb-blocks-goto-execution selected)))))

;;; System Setup and Utilities

(defun org-duckdb-blocks-register-advice (&rest _)
  "Advice function that registers DuckDB block execution.
This is added as :before advice to `org-babel-execute-src-block'
to capture block state at the moment of execution.

The function checks if we're in a DuckDB block and registers it
if so. The registration info is used by async execution and
history tracking. Arguments are ignored since we get context
from point position."
  (when (org-in-src-block-p)
    (let* ((el (org-element-context))
           ;; Force full element parse to ensure all properties are available
           (el-full (org-element-at-point))
           (is-duckdb (and (eq (car el) 'src-block)
                          (string= (org-element-property :language el) "duckdb"))))
      (when is-duckdb
        (org-duckdb-blocks-register-execution)))))

(defun org-duckdb-blocks-setup ()
  "Initialize the DuckDB block tracking system.
Adds advice to org-babel-execute-src-block to register executions.
This is the main entry point for using this package and should be
called once during initialization."
  (interactive)
  (unless (advice-member-p 'org-duckdb-blocks-register-advice 'org-babel-execute-src-block)
    (advice-add 'org-babel-execute-src-block :before #'org-duckdb-blocks-register-advice)
    (message "DuckDB block tracking activated")))

(defun org-duckdb-blocks-clear ()
  "Reset all DuckDB block tracking data structures.
Clears the registry, execution history, and circular buffer.
This is useful for testing or if the tracking data becomes corrupted.
Note that this does not remove ID properties from source blocks."
  (interactive)
  (clrhash org-duckdb-blocks-registry)
  (clrhash org-duckdb-blocks-executions)
  (setq org-duckdb-blocks-history-vector (make-vector org-duckdb-blocks-history-capacity nil)
        org-duckdb-blocks-history-index 0
        org-duckdb-blocks-history-count 0)
  (message "DuckDB block tracking data cleared"))

;;; Reporting and Visualization

(defun org-duckdb-blocks-list (&optional limit)
  "Display tracked DuckDB blocks and their executions, up to LIMIT per block.
Creates a formatted report in a help buffer showing all tracked blocks
and their execution history. For each block, shows:
- Block ID and location
- Execution IDs with timestamps
- Execution parameters

If LIMIT is provided, shows at most that many executions per block.
Default limit is 10 executions per block."
  (interactive "P")
  (let ((limit (or limit 10)))
    (with-help-window "*DuckDB Blocks*"
      (princ "DuckDB Blocks and Executions\n==========================\n\n")

      (if (zerop (hash-table-count org-duckdb-blocks-registry))
          (princ "No blocks found.\n")

        ;; Process each block
        (maphash
         (lambda (block-id info)
           (princ (format "Block ID: %s\n" block-id))
           (princ (format "  File: %s\n" (or (plist-get info :file) "N/A")))
           (princ (format "  Buffer: %s\n" (or (plist-get info :buffer) "N/A")))
           (princ (format "  Source block: %d-%d\n"
                          (plist-get info :begin)
                          (plist-get info :end)))

           ;; Find and sort executions
           (let ((executions '()))
             (maphash (lambda (exec-id exec-info)
                        (when (equal (org-duckdb-blocks-normalize-id block-id) (org-duckdb-blocks-normalize-id (plist-get exec-info :block-id)))
                          (push (cons exec-id exec-info) executions)))
                      org-duckdb-blocks-executions)
             (let* ((sorted-execs
                     (sort executions
                           (lambda (a b)
                             (time-less-p (plist-get (cdr b) :time)
                                          (plist-get (cdr a) :time)))))
                    (limited-execs
                     (seq-take sorted-execs (min limit (length sorted-execs))))
                    (total-count (length sorted-execs)))
               (princ (format "  Executions (%d total):\n" total-count))
               (if limited-execs
                   (progn
                     (dolist (exec limited-execs)
                       (let* ((exec-id (car exec))
                              (exec-info (cdr exec))
                              (params (plist-get exec-info :parameters)))
                         (princ (format "    %s (%s)%s\n"
                                        exec-id
                                        (format-time-string "%Y-%m-%d %H:%M:%S.%3N"
                                                           (plist-get exec-info :time))
                                        (if params (format " %s" params) "")))))
                     (when (> total-count limit)
                       (princ (format "    ... and %d more\n" (- total-count limit)))))
                 (princ "    None\n"))))

           (princ "\n"))
         org-duckdb-blocks-registry)))))

(defun org-duckdb-blocks-recent (&optional limit)
  "Display recent DuckDB executions chronologically, up to LIMIT entries.
Shows a chronological list of recent executions from newest to oldest.
For each execution, displays:
- Timestamp
- Block ID or name
- File and buffer location
- Parameters used

This provides a time-based view of DuckDB activity across all files.
Default limit is 10 executions."
  (interactive "P")
  (let ((limit (or limit 10)))
    (with-help-window "*Recent DuckDB Executions*"
      (princ "Recent DuckDB Executions\n=======================\n\n")
      (let ((recent (org-duckdb-blocks-get-recent-history limit)))
        (if (not recent)
            (princ "No recent executions found.\n")
          (dolist (entry recent)
            (let* ((exec-id (aref entry 0))
                   (block-id (aref entry 1))
                   (timestamp (aref entry 2))
                   (exec-info (gethash (org-duckdb-blocks-normalize-id exec-id) org-duckdb-blocks-executions))
                   (block-info (gethash (org-duckdb-blocks-normalize-id block-id) org-duckdb-blocks-registry)))

              (princ (format "[%s] Block: %s\n"
                             (format-time-string "%Y-%m-%d %H:%M:%S.%3N" timestamp)
                             (or (plist-get exec-info :name)
                                 (substring (org-duckdb-blocks-normalize-id block-id) 0 12))))
              (princ (format "    Execution: %s\n" exec-id))

              (when block-info
                (princ (format "    File: %s\n" (or (plist-get block-info :file) "N/A")))
                (princ (format "    Buffer: %s\n" (or (plist-get block-info :buffer) "N/A"))))

              ;; Add parameter information if available
              (when-let ((params (plist-get exec-info :parameters)))
                (princ (format "    Parameters: %s\n" params)))
              (when-let ((header (plist-get exec-info :header)))
                (princ (format "    Header: %s...\n"
                               (substring (format "%S" header) 0
                                          (min 60 (length (format "%S" header)))))))

              (princ "\n"))))))))

(provide 'org-duckdb-blocks)

;;; org-duckdb-blocks.el ends here
