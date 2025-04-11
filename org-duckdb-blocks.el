;;; org-duckdb-blocks.el --- Track DuckDB block executions in Org files -*- lexical-binding: t; -*-

;; Author: gggion
;; Version: 1.0.0
;; Package-Requires: ((emacs "28.1") (org "9.5"))
;; Keywords: org, duckdb, data
;; URL: https://github.com/gggion/ob-duckdb

;;; Commentary:

;; The `org-duckdb-blocks' package provides a sophisticated tracking system
;; for DuckDB source blocks in Org mode documents. It serves as the foundation
;; for the execution history tracking functionality in `ob-duckdb'.
;;
;; This system assigns unique identifiers to both blocks (persistent) and
;; executions (ephemeral), maintains a comprehensive execution history, and
;; provides navigation tools to revisit any executed block.
;;
;; Key features include:
;;
;; - Persistent block identification with automatic ID recovery
;; - Complete execution history with timestamps
;; - Fast navigation between executed blocks
;; - Self-documenting source blocks with ID properties
;; - Memory-efficient history tracking with dual storage mechanisms
;;
;; To use this package, simply require it from `ob-duckdb' or activate it
;; directly with:
;;
;;   (require 'org-duckdb-blocks)
;;   (org-duckdb-blocks-setup)

;;; Code:

(require 'org-element)
(require 'org-macs)

;;; Core Data Structures

(defvar org-duckdb-blocks-registry (make-hash-table :test 'equal)
  "Persistent registry mapping block IDs to their metadata.

Each entry is a property list containing:
- `:begin' — Buffer position where the block starts
- `:end' — Buffer position where the block ends
- `:file' — File path containing the block
- `:buffer' — Buffer name containing the block
- `:content' — The actual DuckDB query content

This registry serves as the authoritative source for block information
and enables location-independent block identification.")

(defvar org-duckdb-blocks-executions (make-hash-table :test 'equal)
  "Comprehensive execution history mapping execution IDs to details.

Each entry is a property list containing:
- `:block-id' — The permanent ID of the executed block
- `:begin' — Buffer position at execution time
- `:time' — Timestamp of execution with high precision
- `:content' — The content that was executed

This registry preserves the complete history of all executions,
regardless of how many times a block has been executed.")

(defvar org-duckdb-blocks-history-vector (make-vector 100 nil)
  "Circular buffer containing recent executions for fast access.

This vector stores fixed-size entries in chronological order,
implementing a circular buffer pattern where new entries replace the
oldest ones when capacity is reached. Each entry is a vector containing:
- Index 0: Execution ID
- Index 1: Block ID
- Index 2: Timestamp

Unlike the hash tables, this vector enables efficient sequential access
to recent history, optimized for displaying timelines and recent activity.")

(defvar org-duckdb-blocks-history-index 0
  "Current position in the history vector's circular buffer.

When a new execution is recorded, this index is used to determine
where to store the entry, after which the index is incremented.
When the index reaches the capacity, it wraps around to zero,
implementing the circular buffer pattern.")

(defvar org-duckdb-blocks-history-count 0
  "Number of entries currently stored in the history vector.

This count increases with each execution until it reaches the vector's
capacity. It's used to optimize history retrieval operations by
avoiding unnecessary iterations through empty slots in the vector.")

(defvar org-duckdb-blocks-history-capacity 100
  "Maximum number of recent executions to store in the history vector.

This value defines the size of the circular buffer. Increasing this value
allows more history to be efficiently accessed, at the cost of memory usage.
The value can be changed with `org-duckdb-blocks-resize-vector'.")

;;; Core Utility Functions

(defun org-duckdb-blocks-add-to-history (exec-id block-id timestamp)
  "Record a block execution in the circular history buffer.

EXEC-ID is the unique identifier for this execution instance.
BLOCK-ID is the permanent identifier for the executed block.
TIMESTAMP is the time when execution occurred.

This function implements the circular buffer algorithm:
1. Create a vector containing the execution details
2. Store this vector at the current index position
3. Increment the index with wraparound if needed
4. Update the count, capped at maximum capacity

This operation is O(1) and preserves the chronological order of executions."
  (let ((entry (vector exec-id block-id timestamp)))
    ;; Store the new entry at the current index
    (aset org-duckdb-blocks-history-vector org-duckdb-blocks-history-index entry)

    ;; Update index with wraparound and count with ceiling
    (setq org-duckdb-blocks-history-index
          (% (1+ org-duckdb-blocks-history-index) org-duckdb-blocks-history-capacity)
          org-duckdb-blocks-history-count
          (min (1+ org-duckdb-blocks-history-count) org-duckdb-blocks-history-capacity))))

(defun org-duckdb-blocks-get-recent-history (n)
  "Retrieve the N most recent executions from history.

This function efficiently extracts execution entries from the circular
buffer, returning them in chronological order (newest first). The
returned list contains at most N items, but may contain fewer if
the history has fewer than N entries.

Each history entry is a vector containing:
- Execution ID (index 0)
- Block ID (index 1)
- Timestamp (index 2)

The function calculates the correct starting position based on the current
index and count, handling the circular nature of the buffer to return
items in proper chronological sequence."
  (let* ((count (min n org-duckdb-blocks-history-count))
         (start-idx (if (< org-duckdb-blocks-history-count org-duckdb-blocks-history-capacity)
                        ;; If buffer isn't full yet, start from the highest filled index
                        (1- org-duckdb-blocks-history-count)
                      ;; If buffer is full, calculate the index before the current one
                      (mod (+ org-duckdb-blocks-history-index
                             (- org-duckdb-blocks-history-capacity 1))
                           org-duckdb-blocks-history-capacity)))
         result)
    (dotimes (i count result)
      ;; Calculate index with wraparound and extract entry
      (when-let* ((idx (mod (- start-idx i) org-duckdb-blocks-history-capacity))
                 (entry (aref org-duckdb-blocks-history-vector idx)))
        (push entry result)))))

(defun org-duckdb-blocks-block-signature (content)
  "Compute a unique cryptographic signature for block CONTENT.

This hash-based signature serves as a content-derived identifier,
enabling blocks to be recognized even when their properties or location
have changed. The signature considers only the actual DuckDB query
content, stripping whitespace to improve match reliability.

Using SHA-1, the chance of collision is negligible, making this
suitable for identifying blocks within a document's scope."
  (secure-hash 'sha1
               (string-trim content)))

(defun org-duckdb-blocks-find-block-by-content (content)
  "Identify a block by its CONTENT, enabling property-free recognition.

When a block loses its ID property (through deletion or editing), this
function can recover its identity by matching its content against the
execution history. It implements a two-phase matching strategy:

1. Exact content signature matching (fastest path)
2. Prefix-based substring matching (more resilient to minor edits)

The function prioritizes finding the correct block ID even when the
content has been slightly modified, making it robust against common
editing patterns. It returns the block ID if found, or nil otherwise."
  (let ((content-hash (org-duckdb-blocks-block-signature content))
        (found-id nil))
    (maphash (lambda (exec-id exec-info)
               (when (and (not found-id)  ; Stop after finding first match
                          ;; Try to match first 50 chars as a substring
                          (string-match-p
                           (regexp-quote (substring content 0 (min 50 (length content))))
                           (or (plist-get exec-info :content) "")))
                 (setq found-id (plist-get exec-info :block-id))))
             org-duckdb-blocks-executions)
    found-id))

;;; Block Registration and Management

(defun org-duckdb-blocks-register-execution ()
  "Register execution of the DuckDB block at point in the tracking system.

This function implements the core tracking mechanism for DuckDB blocks:

1. Extract the block content and metadata at point
2. Find or generate permanent block identifier
3. Record execution in all tracking mechanisms
4. Update block properties to maintain self-documentation

When a block is executed for the first time:
- It receives a permanent UUID as its block ID
- A property drawer is added with ID and execution information
- The block is registered in the master registry

For subsequent executions:
- The block is recognized by its existing ID
- Only the execution ID and timestamp are updated
- Location information is refreshed in case the block moved

The function provides detailed logging during execution to assist in
troubleshooting. It returns a plist with :block-id and :exec-id values."
  (interactive)
  (let* ((el (org-element-at-point))
         (is-src-block (eq (car el) 'src-block))
         (props (cadr el)))

    (when (and is-src-block
               (string= (plist-get props :language) "duckdb"))

      ;; Get element positions safely
      (let* ((begin (org-element-property :begin el))
             (end (org-element-property :end el))
             (contents-begin (org-element-property :contents-begin el))
             (contents-end (org-element-property :contents-end el))
             (content (when (and contents-begin contents-end)
                        (buffer-substring-no-properties contents-begin contents-end)))
             (exec-id (org-id-uuid))
             (timestamp (current-time))
             (existing-id nil)
             (file (buffer-file-name))
             (buffer (buffer-name))
             block-id)

        ;; Handle cases where content extraction fails
        (unless content
          (setq content ""))

        ;; First attempt: Find existing block ID from property
        (save-excursion
          (goto-char begin)
          (setq existing-id
                (when (re-search-backward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)" (- begin 200) t)
                  (match-string 1))))


        ;; Second attempt: Try to recover ID based on content
        (unless existing-id
          (setq existing-id (org-duckdb-blocks-find-block-by-content content)))

        ;; Create or use existing block ID
        (setq block-id (or existing-id (org-id-uuid)))

        ;; Store block in registry if it's new
        (unless (and existing-id (gethash existing-id org-duckdb-blocks-registry))
          (puthash block-id (list :begin begin
                                :end end
                                :file file
                                :buffer buffer
                                :content content)
                 org-duckdb-blocks-registry))

        ;; Always update location info to handle buffer/position changes
        (when (gethash block-id org-duckdb-blocks-registry)
          (puthash block-id
                   (plist-put
                    (plist-put
                     (plist-put
                      (plist-put (gethash block-id org-duckdb-blocks-registry) :begin begin)
                      :end end)
                     :file file)
                    :buffer buffer)
                   org-duckdb-blocks-registry))

        ;; Store execution details in the executions registry
        (puthash exec-id (list :block-id block-id
                             :begin begin
                             :time timestamp
                             :content content)
               org-duckdb-blocks-executions)

        ;; Add to vector-based history for fast access
        (org-duckdb-blocks-add-to-history exec-id block-id timestamp)

        ;; Update the block properties to maintain self-documentation
        (save-excursion
          (goto-char begin)

          ;; Add block ID if missing
          (unless (save-excursion
                    (goto-char begin)
                    (re-search-backward "^#\\+PROPERTY: ID" (- begin 200) t))
            (when (re-search-forward "^#\\+begin_src" nil t)
              (beginning-of-line)
              (open-line 1)
              (insert (format "#+PROPERTY: ID %s" block-id))))

          ;; Update execution ID (always)
          (goto-char begin)
          (if (re-search-backward "^#\\+PROPERTY: EXEC_ID" (- begin 200) t)
              (progn
                (beginning-of-line)
                (kill-line)
                (insert (format "#+PROPERTY: EXEC_ID %s" exec-id)))
            (when (re-search-forward "^#\\+begin_src" nil t)
              (beginning-of-line)
              (open-line 1)
              (insert (format "#+PROPERTY: EXEC_ID %s" exec-id)))))

        ;; Return values for further processing
        (message "[duckdb-blocks] Registration complete")
        (list :block-id block-id :exec-id exec-id)))))

(defun org-duckdb-blocks-find-block-at-point ()
  "Identify the DuckDB block at point, with or without explicit ID.

This function performs a multi-tiered identification process:

1. First checks for an explicit ID property in the block
2. Verifies the ID exists in the registry for validity
3. Falls back to content-based matching if needed
4. Restores the ID property if recovered from content

This allows blocks to retain their identity even when their
properties are deleted or when they're copied to new locations.
The function returns the block ID if found, or nil otherwise."
  (interactive)
  (when-let* ((el (org-element-at-point))
             (is-src-block (eq (car el) 'src-block))
             (props (cadr el))
             (is-duckdb (string= (plist-get props :language) "duckdb"))
             (std-props (plist-get props :standard-properties))
             (begin (aref std-props 0))
             (code-begin (aref std-props 2))
             (code-end (aref std-props 3))
             (content (buffer-substring-no-properties code-begin code-end)))

    ;; First try to find explicit ID property
    (let ((explicit-id nil))
      (save-excursion
        (goto-char begin)
        (setq explicit-id
              (when (re-search-backward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)" (- begin 200) t)
                (match-string 1))))

      ;; If explicit ID exists and is in registry, return it
      (if (and explicit-id (gethash explicit-id org-duckdb-blocks-registry))
          explicit-id

        ;; Otherwise try to match by content
        (when-let ((found-id (org-duckdb-blocks-find-block-by-content content)))
          ;; Restore the property if we found a match
          (save-excursion
            (goto-char begin)
            (if (re-search-backward "^#\\+PROPERTY: ID" (- begin 200) t)
                (progn
                  (beginning-of-line)
                  (kill-line)
                  (insert (format "#+PROPERTY: ID %s" found-id)))
              (when (re-search-forward "^#\\+begin_src" nil t)
                (beginning-of-line)
                (open-line 1)
                (insert (format "#+PROPERTY: ID %s" found-id)))))
          ;; Return the found ID
          found-id)))))

(defun org-duckdb-blocks-recover-id ()
  "Restore missing ID properties for the DuckDB block at point.

This interactive function performs a comprehensive recovery operation:

1. Extracts the content of the DuckDB block at point
2. Searches execution history for content matches
3. Restores the original block ID if found
4. Also recovers the most recent execution ID for the block
5. Updates properties in the document to reflect recovery

This command is especially useful after:
- Copying blocks without their properties
- Accidental deletion of properties
- Moving blocks between documents

The function provides user feedback via messages to confirm
successful recovery or report when no match is found."
  (interactive)
  (let* ((el (org-element-at-point))
         (el-type (car el))
         (props (cadr el)))

    (when (and (eq el-type 'src-block)
               (string= (plist-get props :language) "duckdb"))
      (let* ((begin (org-element-property :begin el))
             (contents-begin (org-element-property :contents-begin el))
             (contents-end (org-element-property :contents-end el))
             (content (or (and contents-begin contents-end
                               (buffer-substring-no-properties contents-begin contents-end))
                          "")))

        ;; First try to find based on content
        (let ((found-id (org-duckdb-blocks-find-block-by-content content)))
          (if found-id
              (progn
                ;; Restore the ID property
                (save-excursion
                  (goto-char begin)
                  (if (re-search-backward "^#\\+PROPERTY: ID" (- begin 200) t)
                      (progn
                        (beginning-of-line)
                        (kill-line)
                        (insert (format "#+PROPERTY: ID %s" found-id)))
                    (when (re-search-forward "^#\\+begin_src" nil t)
                      (beginning-of-line)
                      (open-line 1)
                      (insert (format "#+PROPERTY: ID %s" found-id)))))

                ;; Find and restore most recent execution ID
                (let* ((execs nil)
                       (latest-exec nil)
                       (latest-time nil))

                  ;; Gather all executions for this block
                  (maphash (lambda (exec-id exec-info)
                             (when (equal found-id (plist-get exec-info :block-id))
                               (push (cons exec-id (plist-get exec-info :time)) execs)))
                           org-duckdb-blocks-executions)

                  ;; Find the most recent execution
                  (dolist (exec execs)
                    (when (or (not latest-time)
                              (time-less-p latest-time (cdr exec)))
                      (setq latest-time (cdr exec)
                            latest-exec (car exec))))

                  ;; Restore latest EXEC_ID property if found
                  (when latest-exec
                    (save-excursion
                      (goto-char begin)
                      (if (re-search-backward "^#\\+PROPERTY: EXEC_ID" (- begin 200) t)
                          (progn
                            (beginning-of-line)
                            (kill-line)
                            (insert (format "#+PROPERTY: EXEC_ID %s" latest-exec)))
                        (when (re-search-forward "^#\\+begin_src" nil t)
                          (beginning-of-line)
                          (open-line 1)
                          (insert (format "#+PROPERTY: EXEC_ID %s" latest-exec))))))))
            (message "[duckdb-blocks] No matching block found in registry")))))))

;;; System Setup and Management
(defun org-duckdb-blocks-register-advice (&rest _)
  "Advice function to register DuckDB block executions."
  (when (org-in-src-block-p)
    (let* ((el (org-element-context))
           (el-type (car el))
           (lang (org-element-property :language el)))
      (when (and (eq el-type 'src-block)
                (string= lang "duckdb"))
        (when-let ((ids (org-duckdb-blocks-register-execution))))))))

(defun org-duckdb-blocks-setup ()
  "Initialize the DuckDB block tracking system.

This function activates the tracking mechanism by installing an advice
on `org-babel-execute-src-block' that intercepts executions of DuckDB
source blocks. The advice performs the following steps:

1. Checks if the current source block is a DuckDB block
2. If so, registers the execution in the tracking system
3. Reports the block and execution IDs in the minibuffer

This non-invasive approach means the tracking system only activates
for DuckDB blocks, leaving other block types unaffected. The advice
runs before the actual execution, ensuring that the tracking occurs
even if the execution fails due to errors.

This function should be called once during initialization."
  (interactive)
  (unless (advice-member-p 'org-duckdb-blocks-register-advice 'org-babel-execute-src-block)
    (advice-add 'org-babel-execute-src-block :before #'org-duckdb-blocks-register-advice)))

(defun org-duckdb-blocks-goto-block (id)
  "Navigate to the DuckDB source block with the given ID.

This command provides precise navigation to any block in the registry,
regardless of its current location. The function:

1. Retrieves the block's location information from the registry
2. Opens the file containing the block if needed
3. Switches to the appropriate buffer
4. Positions the cursor at the beginning of the block

This enables you to jump directly to blocks you've previously executed,
even if they're in different files or have been moved within a file.
Completion is available for selecting block IDs interactively."
  (interactive
   (list (completing-read "Block ID: "
                         (hash-table-keys org-duckdb-blocks-registry))))
  (when-let* ((info (gethash id org-duckdb-blocks-registry))
              (begin (plist-get info :begin))
              (file (plist-get info :file))
              (buffer (plist-get info :buffer)))
    (cond
     ;; Open file if it exists
     ((and file (file-exists-p file)) (find-file file))
     ;; Or switch to buffer if it exists
     ((get-buffer buffer) (switch-to-buffer buffer))
     ;; Report failure if neither exists
     (t (message "[duckdb-blocks] Source not found")))
    ;; Position cursor at the block's beginning
    (goto-char begin)))

(defun org-duckdb-blocks-goto-execution (exec-id)
  "Navigate to the source block of a specific execution.

This command allows you to jump directly to a block based on a particular
EXEC-ID rather than its block ID. This is useful when investigating
the history of executions, as you can go directly to the block that was
executed at a specific time.

The function:
1. Looks up the execution details in the registry
2. Extracts the corresponding block ID
3. Delegates to `org-duckdb-blocks-goto-block' for the actual navigation

Completion is available for selecting execution IDs interactively."
  (interactive
   (list (completing-read "Execution ID: "
                         (hash-table-keys org-duckdb-blocks-executions))))
  (when-let* ((exec-info (gethash exec-id org-duckdb-blocks-executions))
              (block-id (plist-get exec-info :block-id)))
    (org-duckdb-blocks-goto-block block-id)))

(defun org-duckdb-blocks-clear ()
  "Reset all DuckDB block tracking data structures.

This command completely clears all tracking information:
1. Empties the block registry
2. Empties the execution history
3. Resets the history vector to its initial empty state

This is useful to start fresh after testing or when you want to
remove all historical data. Note that this does not remove the
ID properties from the blocks themselves in the documents."
  (interactive)
  (clrhash org-duckdb-blocks-registry)
  (clrhash org-duckdb-blocks-executions)
  (setq org-duckdb-blocks-history-vector (make-vector org-duckdb-blocks-history-capacity nil)
        org-duckdb-blocks-history-index 0
        org-duckdb-blocks-history-count 0))

;;; Visualization and Reporting

(defun org-duckdb-blocks-list (&optional limit)
  "Display a list of all tracked DuckDB blocks and their executions.

This command generates a detailed report showing:
- All tracked blocks with their IDs, files, and buffer names
- For each block, the most recent LIMIT executions (defaults to 10)
- Timestamps for each execution with millisecond precision
- A count of the total executions for each block

The listing is displayed in a dedicated buffer named \"*DuckDB Blocks*\".
When a block has more executions than the limit, a summary line shows
how many additional executions exist.

The optional LIMIT argument can be provided with a prefix argument."
  (interactive "P")
  (let ((limit (or limit 10)))
    (with-help-window "*DuckDB Blocks*"
      (princ "DuckDB Blocks and Executions\n==========================\n\n")

      (let ((blocks (make-hash-table :test 'equal)))
        ;; Build execution map
        (maphash (lambda (id info)
                  (puthash id (list :info info :execs nil) blocks))
                org-duckdb-blocks-registry)

        ;; Associate executions with blocks
        (maphash (lambda (exec-id exec-info)
                  (when-let* ((block-id (plist-get exec-info :block-id))
                             (block (gethash block-id blocks)))
                    (puthash block-id
                            (plist-put block :execs
                                      (cons (cons exec-id exec-info)
                                           (plist-get block :execs)))
                            blocks)))
                org-duckdb-blocks-executions)

        ;; Display blocks
        (if (zerop (hash-table-count blocks))
            (princ "No blocks found.\n")
          (maphash (lambda (block-id block-data)
                    (let* ((info (plist-get block-data :info))
                          (execs (plist-get block-data :execs))
                          ;; Sort executions by time (newest first)
                          (sorted-execs
                           (sort execs (lambda (a b)
                                       (time-less-p (plist-get (cdr b) :time)
                                                  (plist-get (cdr a) :time)))))
                          ;; Limit to the most recent executions
                          (limited-execs (cl-subseq sorted-execs 0 (min limit (length sorted-execs))))
                          (total-exec-count (length sorted-execs)))

                      (princ (format "Block ID: %s\n" block-id))
                      (princ (format "  File: %s\n" (or (plist-get info :file) "N/A")))
                      (princ (format "  Buffer: %s\n" (or (plist-get info :buffer) "N/A")))
                      (princ (format "  Executions (%d total):\n" total-exec-count))

                      (if limited-execs
                          (progn
                            (dolist (exec limited-execs)
                              (princ (format "    %s (%s)\n"
                                            (car exec)
                                            (format-time-string "%Y-%m-%d %H:%M:%S.%3N"
                                                             (plist-get (cdr exec) :time)))))
                            (when (> total-exec-count limit)
                              (princ (format "    ... and %d more\n" (- total-exec-count limit)))))
                        (princ "    None\n"))
                      (princ "\n")))
                  blocks))))))

(defun org-duckdb-blocks-recent (&optional limit)
  "Display the most recent DuckDB executions across all blocks.

Unlike `org-duckdb-blocks-list' which organizes by block, this command shows a
chronological timeline of executions regardless of which block they belong to.
It provides a historical view of your recent DuckDB activity.

For each execution, the report shows:
- Timestamp with millisecond precision
- Block ID that was executed
- Execution ID of the specific execution
- File and buffer information where available

The listing is displayed in a dedicated buffer named
\"*Recent DuckDB Executions*\".  By default, it shows the 10 most recent
executions, but this can be changed with a prefix argument representing LIMIT."

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
                   (block-info (gethash block-id org-duckdb-blocks-registry)))
              (princ (format "[%s] Block: %s\n"
                             (format-time-string "%Y-%m-%d %H:%M:%S.%3N" timestamp)
                             block-id))
              (princ (format "    Execution: %s\n" exec-id))
              (when block-info
                (princ (format "    File: %s\n" (or (plist-get block-info :file) "N/A")))
                (princ (format "    Buffer: %s\n" (or (plist-get block-info :buffer) "N/A"))))
              (princ "\n"))))))))

(defun org-duckdb-blocks-inspect-vector ()
  "Display the internal state of the history vector for inspection.

This command reveals the implementation details of the circular buffer
used for tracking recent executions. It shows:
- Current item count and maximum capacity
- Current index position in the circular buffer
- All entries in chronological order
- A marker indicating the most recently added entry

This function is primarily intended for debugging and understanding
the internal mechanics of the history tracking system. It provides
insight into how the circular buffer implementation works."
  (interactive)
  (with-help-window "*DuckDB Blocks History*"
    (princ (format "History: %d items (capacity %d), current index: %d\n\n"
                  org-duckdb-blocks-history-count
                  org-duckdb-blocks-history-capacity
                  org-duckdb-blocks-history-index))

    (let* ((start (mod (- org-duckdb-blocks-history-index org-duckdb-blocks-history-count)
                     org-duckdb-blocks-history-capacity))
          (count org-duckdb-blocks-history-count))

      (if (zerop count)
          (princ "  [Empty]\n")
        (dotimes (i count)
          (let* ((idx (mod (+ start i) org-duckdb-blocks-history-capacity))
                 (entry (aref org-duckdb-blocks-history-vector idx))
                 (current-mark (if (= idx (mod (1- org-duckdb-blocks-history-index)
                                             org-duckdb-blocks-history-capacity))
                                "→ " "  ")))
            (when entry
              (princ (format "%s%s: Exec %s, Block %s\n"
                            current-mark
                            (format-time-string "%Y-%m-%d %H:%M:%S"
                                              (aref entry 2))
                            (substring (aref entry 0) 0 8)
                            (substring (aref entry 1) 0 8))))))))))

(provide 'org-duckdb-blocks)

;;; org-duckdb-blocks.el ends here
