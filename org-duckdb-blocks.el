;; org-duckdb-blocks.el --- Track DuckDB block executions in Org files -*- lexical-binding: t; -*-

;; Author: gggion
;; Package-Requires: ((emacs "28.1") (org "9.5"))
;; Keywords: org, duckdb, data
;; URL: https://github.com/gggion/ob-duckdb

;;; Commentary:
;;
;; This ob-duckdb addon tracks DuckDB source blocks in Org mode documents,
;; providing, execution history, block identification, and navigation
;; functionality.
;;
;; The tracking system provides useful execution history and navigation
;; for debugging purposes.
;;
;; Basic usage:
;;
;;     (require 'org-duckdb-blocks)
;;     (setq org-duckdb-blocks-enable-tracking t)
;;     (org-duckdb-blocks-setup)
;;
;; This automatically tracks all DuckDB block executions by observing
;; ob-duckdb.el hooks.
;;
;; Key features:
;; - Persistent block identity via buffer-local properties
;; - Execution history with timestamps and content snapshots
;; - Position tracking that survives document edits
;; - Navigation commands to revisit executed blocks
;; - Automatic cleanup of stale entries
;; - Process tracking for cancellation support
;;
;; The package maintains three data structures:
;; - Registry: Maps block IDs to current positions and content
;; - Executions: Maps execution IDs to execution details
;; - History: Circular buffer of recent executions for fast access
;;
;; Navigation commands:
;; - `org-duckdb-blocks-navigate-recent' - Browse recent executions
;; - `org-duckdb-blocks-goto-block' - Jump to block by ID
;; - `org-duckdb-blocks-goto-execution' - Jump to execution by ID
;;
;; Reporting commands:
;; - `org-duckdb-blocks-list' - Show all tracked blocks
;; - `org-duckdb-blocks-recent' - Show recent executions
;; - `org-duckdb-blocks-execution-info' - Detailed execution information
;;
;;; Code:

(require 'org-element)
(require 'org-macs)
(require 'org-id)

;;;; Customizable vars
(defcustom org-duckdb-blocks-enable-tracking nil
  "Enable execution history and block tracking for debugging.

When nil, ob-duckdb works with minimal state - only tracks pending
async executions until completion via `org-babel-duckdb--pending-async'.

When non-nil, enables full debugging features:
- Execution history via `org-duckdb-blocks-executions'
- Block registry via `org-duckdb-blocks-registry'
- Navigation commands (`org-duckdb-blocks-navigate-recent')
- Cancellation support (`org-babel-duckdb-cancel-execution')

Enabling tracking also controls property insertion via
`org-duckdb-blocks-visible-properties'.

Default is nil for minimal intrusion.

Also see `org-duckdb-blocks-setup' to initialize tracking."
  :type 'boolean
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "2.0.0"))

(defcustom org-duckdb-blocks-visible-properties nil
  "Show block/exec IDs as visible #+PROPERTY: lines.

When nil (default), stores IDs as invisible text properties to avoid
document clutter.

When non-nil, inserts visible #+PROPERTY: ID and #+PROPERTY: EXEC_ID
lines before source blocks for inspection and debugging.

Only relevant when `org-duckdb-blocks-enable-tracking' is non-nil.

Text property storage enables full tracking features without polluting
document source.

Also see `org-duckdb-blocks-update-properties' for property insertion."
  :type 'boolean
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "2.0.0"))

;;;; Utility Functions

(defun org-duckdb-blocks-normalize-id (id)
  "Return ID as plain string without text properties.
Critical for consistent hashtable lookups since text properties cause
string equality comparisons to fail.

All IDs must be normalized before storage or comparison.
Used by all functions that handle block or execution IDs."
  (if (stringp id) (substring-no-properties id) id))

;;;; Core Data Structures

(defvar org-duckdb-blocks-registry (make-hash-table :test 'equal)
  "Registry mapping block IDs to current metadata.

Keys are block ID strings (UUIDs).
Values are plists with:
  :begin     - Position of block start
  :end       - Position of block end
  :file      - File containing the block
  :buffer    - Buffer name containing the block
  :content   - Current block SQL content

Managed by `org-duckdb-blocks-register-execution' and
`org-duckdb-blocks-update-all-block-positions'.

Queried by `org-duckdb-blocks-goto-block' for navigation.
Cleaned by `org-duckdb-blocks-clear'.")

(defvar org-duckdb-blocks-executions (make-hash-table :test 'equal)
  "Execution history mapping execution IDs to details.

Keys are execution ID strings (UUIDs).
Values are plists with:
  :block-id   - ID of executed block (see `org-duckdb-blocks-registry')
  :begin      - Block position at execution time
  :time       - Timestamp of execution
  :content    - SQL content executed (snapshot)
  :parameters - Header parameters string
  :header     - Parsed header arguments
  :switches   - Block switches
  :line-count - Number of lines in content
  :name       - Block name (if any)
  :status     - Execution status (see `org-duckdb-blocks-execution-status')
  :process    - Process object (if running, `org-duckdb-blocks-store-process')
  :error-info - Error details (if status is \\='error)

Populated by `org-duckdb-blocks-register-execution'.
Queried by `org-duckdb-blocks-execution-info' and navigation commands.
Also see `org-duckdb-blocks-history-vector' for fast recent access.")

(defvar org-duckdb-blocks-history-vector (make-vector 100 nil)
  "Circular buffer of recent executions for O(1) access.

Each slot contains a vector [exec-id block-id timestamp] or nil.
Provides fast access to recent executions without scanning
`org-duckdb-blocks-executions'.

Size controlled by `org-duckdb-blocks-history-capacity'.
Current write position in `org-duckdb-blocks-history-index'.
Entry count in `org-duckdb-blocks-history-count'.

Updated by `org-duckdb-blocks-add-to-history'.
Queried by `org-duckdb-blocks-get-recent-history'.")

(defvar org-duckdb-blocks-history-index 0
  "Current write position in history circular buffer.
Points to where next execution will be written in
`org-duckdb-blocks-history-vector'.

Incremented modulo `org-duckdb-blocks-history-capacity' by
`org-duckdb-blocks-add-to-history'.")

(defvar org-duckdb-blocks-history-count 0
  "Number of valid entries in history buffer.
Used to determine how many entries exist before buffer fills.
Capped at `org-duckdb-blocks-history-capacity'.

Updated by `org-duckdb-blocks-add-to-history'.
Used by `org-duckdb-blocks-get-recent-history'.")

(defvar org-duckdb-blocks-history-capacity 100
  "Maximum entries in history circular buffer.
Limits `org-duckdb-blocks-history-vector' size.

Change requires calling `org-duckdb-blocks-clear' to resize buffer.")

(defvar org-duckdb-blocks-execution-status (make-hash-table :test 'equal)
  "Execution status tracking by execution ID.

Keys are execution ID strings.
Values are status symbols: running, completed, cancelled, error,
warning, completed-with-errors.

Updated by `org-duckdb-blocks-update-execution-status'.
Queried by `org-duckdb-blocks-get-execution-status' and
`org-duckdb-blocks-get-running-executions'.")

;;;; History Management

(defun org-duckdb-blocks-add-to-history (exec-id block-id timestamp)
  "Add execution record to circular history buffer.

EXEC-ID and BLOCK-ID are normalized before storage.
TIMESTAMP is from `current-time'.

Overwrites oldest entry when buffer is full.

Updates `org-duckdb-blocks-history-vector',
`org-duckdb-blocks-history-index', and
`org-duckdb-blocks-history-count'.

Called by `org-duckdb-blocks-register-execution'.
Retrieved via `org-duckdb-blocks-get-recent-history'."
  (let ((entry (vector (org-duckdb-blocks-normalize-id exec-id)
                       (org-duckdb-blocks-normalize-id block-id)
                       timestamp)))
    (aset org-duckdb-blocks-history-vector org-duckdb-blocks-history-index entry)
    (setq org-duckdb-blocks-history-index
          (% (1+ org-duckdb-blocks-history-index) org-duckdb-blocks-history-capacity)
          org-duckdb-blocks-history-count
          (min (1+ org-duckdb-blocks-history-count) org-duckdb-blocks-history-capacity))))

(defun org-duckdb-blocks-get-recent-history (n)
  "Retrieve N most recent executions, newest first.

Returns list of vectors [exec-id block-id timestamp].
Returns fewer than N entries if buffer not yet full.

Efficiently accesses `org-duckdb-blocks-history-vector' without
scanning `org-duckdb-blocks-executions'.

Used by `org-duckdb-blocks-navigate-recent', `org-duckdb-blocks-recent',
and `org-duckdb-blocks-show-execution-status'."
  (let* ((count (min n org-duckdb-blocks-history-count))
         (start-idx (if (< org-duckdb-blocks-history-count org-duckdb-blocks-history-capacity)
                        (1- org-duckdb-blocks-history-count)
                      (mod (+ org-duckdb-blocks-history-index
                             (- org-duckdb-blocks-history-capacity 1))
                           org-duckdb-blocks-history-capacity)))
         result)
    (dotimes (i count result)
      (let* ((idx (mod (- start-idx i) org-duckdb-blocks-history-capacity))
             (entry (aref org-duckdb-blocks-history-vector idx)))
        (when entry (push entry result))))))

;;;; Status Tracking

(defun org-duckdb-blocks-update-execution-status (exec-id status &optional error-info)
  "Update execution STATUS for EXEC-ID with optional ERROR-INFO.

EXEC-ID is normalized before lookup.
STATUS is symbol: running, completed, cancelled, error, warning,
completed-with-errors.
ERROR-INFO is error description string (only for \\='error status).

Updates both `org-duckdb-blocks-execution-status' and :status
field in `org-duckdb-blocks-executions'.

Called by ob-duckdb.el hooks during execution lifecycle.
Queried by `org-duckdb-blocks-get-execution-status'."
  (let ((exec-id (org-duckdb-blocks-normalize-id exec-id)))
    (puthash exec-id status org-duckdb-blocks-execution-status)
    (when-let ((exec-info (gethash exec-id org-duckdb-blocks-executions)))
      (puthash exec-id
               (plist-put exec-info :status status)
               org-duckdb-blocks-executions)
      (when error-info
        (puthash exec-id
                 (plist-put (gethash exec-id org-duckdb-blocks-executions)
                           :error-info error-info)
                 org-duckdb-blocks-executions)))))

(defun org-duckdb-blocks-get-execution-status (exec-id)
  "Get current status for EXEC-ID.
Returns status symbol or nil if not found.

EXEC-ID is normalized before lookup.

Also see `org-duckdb-blocks-update-execution-status' for status values."
  (gethash (org-duckdb-blocks-normalize-id exec-id) org-duckdb-blocks-execution-status))

(defun org-duckdb-blocks-get-latest-exec-id-for-block (begin)
  "Get most recent execution ID for block at BEGIN position.

Scans `org-duckdb-blocks-executions' for executions at position BEGIN,
returns ID of most recent by timestamp.

Used by `org-babel-duckdb-cancel-current-execution' to find the
execution to cancel.

Returns execution ID string or nil if no executions found."
  (let ((latest-exec-id nil)
        (latest-time nil))
    (maphash (lambda (exec-id exec-info)
               (when (= (plist-get exec-info :begin) begin)
                 (let ((exec-time (plist-get exec-info :time)))
                   (when (or (not latest-time) (time-less-p latest-time exec-time))
                     (setq latest-time exec-time
                           latest-exec-id exec-id)))))
             org-duckdb-blocks-executions)
    latest-exec-id))

(defun org-duckdb-blocks-store-process (exec-id process)
  "Store PROCESS reference for EXEC-ID in execution info.

Adds :process field to execution record in `org-duckdb-blocks-executions'.

Used by async execution to enable cancellation via
`org-duckdb-blocks-get-process'.

Called by hook handler when process starts.

Also see `org-babel-duckdb-execute-async' and
`org-babel-duckdb-cancel-execution'."
  (when-let ((exec-info (gethash exec-id org-duckdb-blocks-executions)))
    (puthash exec-id
             (plist-put exec-info :process process)
             org-duckdb-blocks-executions)))

(defun org-duckdb-blocks-get-process (exec-id)
  "Get process for EXEC-ID, or nil if not found.

Retrieves :process field from execution record in
`org-duckdb-blocks-executions'.

Used by `org-babel-duckdb-cancel-execution' to interrupt running queries.

Returns process object or nil."
  (when-let ((exec-info (gethash exec-id org-duckdb-blocks-executions)))
    (plist-get exec-info :process)))

(defun org-duckdb-blocks-get-running-executions ()
  "Return list of execution IDs currently in \\='running status.

Scans `org-duckdb-blocks-executions' for executions with :status \\='running.

Used by `org-babel-duckdb-cancel-execution' for completion candidates.

Also see `org-duckdb-blocks-update-execution-status'."
  (let (running)
    (maphash (lambda (exec-id exec-info)
               (when (eq (plist-get exec-info :status) 'running)
                 (push exec-id running)))
             org-duckdb-blocks-executions)
    running))

;;;; Property Management

(defun org-duckdb-blocks--store-id (block-id exec-id begin)
  "Store BLOCK-ID and EXEC-ID for block at BEGIN position.

Storage method depends on `org-duckdb-blocks-visible-properties':

When nil (default):
  Stores IDs as invisible text properties on #+begin_src line.
  Properties: org-duckdb-block-id, org-duckdb-exec-id.

When non-nil:
  Inserts visible #+PROPERTY: lines before source block.

BEGIN is marker or position of source block start.

Only called when `org-duckdb-blocks-enable-tracking' is non-nil.

Text properties are stored on the #+begin_src line specifically,
not on #+NAME: or #+HEADER: lines, since those may change position
or be added/removed. The #+begin_src line is the stable anchor.

Also see `org-duckdb-blocks-get-block-id' for retrieval and
`org-duckdb-blocks-update-properties' for legacy visible storage."
  (save-excursion
    (goto-char begin)

    (if org-duckdb-blocks-visible-properties
        ;; Visible property lines
        (org-duckdb-blocks-update-properties block-id exec-id begin)

      ;; Invisible text properties - find #+begin_src line
      (let ((src-line-start nil))
        ;; Search forward for #+begin_src from BEGIN
        (when (re-search-forward "^#\\+begin_src duckdb"
                                (save-excursion (forward-line 10) (point))
                                t)
          (setq src-line-start (line-beginning-position))
          (let ((src-line-end (line-end-position)))
            (put-text-property src-line-start src-line-end
                              'org-duckdb-block-id
                              (org-duckdb-blocks-normalize-id block-id))
            (put-text-property src-line-start src-line-end
                              'org-duckdb-exec-id
                              (org-duckdb-blocks-normalize-id exec-id))))))))

(defun org-duckdb-blocks-update-properties (block-id exec-id begin)
  "Update #+PROPERTY: lines for BLOCK-ID and EXEC-ID before block at BEGIN.

Inserts or updates ID and EXEC_ID property lines before source block.
These properties persist block identity across editing sessions and
identify most recent execution.

Both IDs are normalized to prevent text property issues.

Properties are inserted at most 200 characters before BEGIN to avoid
matching properties from other blocks.

Called by `org-duckdb-blocks-register-execution'.
Read by `org-duckdb-blocks-get-block-id'."
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

(defun org-duckdb-blocks-get-block-id (begin)
  "Get block ID from properties near BEGIN, or nil if not found.

Retrieval strategy depends on storage method:

1. Search forward from BEGIN for #+begin_src line and check its
   text properties (fast, default)
2. Search backward for visible #+PROPERTY: ID line (legacy)

BEGIN is position of source block start (from org-element).

Returns normalized ID string or nil.

Used by `org-duckdb-blocks-register-execution' to reuse existing IDs.

Also see `org-duckdb-blocks--store-id' for storage and
`org-duckdb-blocks-visible-properties' for storage mode."
  (save-excursion
    (goto-char begin)
    (or
     ;; Fast path: text property on #+begin_src line
     (when (re-search-forward "^#\\+begin_src duckdb"
                             (save-excursion (forward-line 10) (point))
                             t)
       (get-text-property (line-beginning-position) 'org-duckdb-block-id))

     ;; Fallback: visible property line
     (progn
       (goto-char begin)
       (when (re-search-backward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)"
                                 (max (- begin 200) (point-min)) t)
         (org-duckdb-blocks-normalize-id (match-string 1)))))))

;;;; Block Position Tracking

(defun org-duckdb-blocks-update-all-block-positions ()
  "Synchronize registry with current buffer state.

Updates positions for all tracked blocks in current buffer and removes
stale entries. This critical function ensures registry accuracy as blocks
are added, removed, or edited.

Handles both visible #+PROPERTY: ID lines and invisible text properties
based on `org-duckdb-blocks-visible-properties'.

Procedure:
1. Scan buffer for blocks with IDs (property lines or text properties)
2. Find associated source blocks
3. Update coordinates in `org-duckdb-blocks-registry'
4. Remove registry entries for blocks no longer in buffer
5. Remove duplicate entries for same position

Called by `org-duckdb-blocks-register-execution' before recording
execution to ensure positions are accurate.

Returns list of block IDs currently in buffer."
  (let ((file (buffer-file-name))
        (buffer (buffer-name))
        (found-blocks (make-hash-table :test 'equal))
        (current-buffer-blocks nil))

    ;; First pass: find blocks with IDs in current buffer
    (save-excursion
      (goto-char (point-min))

      (if org-duckdb-blocks-visible-properties
          ;; Scan for visible #+PROPERTY: ID lines
          (while (re-search-forward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)" nil t)
            (let ((block-id (org-duckdb-blocks-normalize-id (match-string 1))))
              (push block-id current-buffer-blocks)

              ;; Find associated source block
              (when (re-search-forward "^#\\+begin_src duckdb" nil t)
                (let* ((element (org-element-at-point))
                       (begin (org-element-property :begin element))
                       (end (org-element-property :end element)))

                  (when (and begin end)
                    (puthash (cons begin end) block-id found-blocks)

                    ;; Update registry
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

        ;; Scan for invisible text properties
        (while (not (eobp))
          (when-let ((block-id (get-text-property (point) 'org-duckdb-block-id)))
            (setq block-id (org-duckdb-blocks-normalize-id block-id))
            (push block-id current-buffer-blocks)

            ;; Check if we're at a source block
            (when (looking-at "^#\\+begin_src duckdb")
              (let* ((element (org-element-at-point))
                     (begin (org-element-property :begin element))
                     (end (org-element-property :end element)))

                (when (and begin end)
                  (puthash (cons begin end) block-id found-blocks)

                  ;; Update registry
                  (let* ((contents-begin (org-element-property :contents-begin element))
                         (contents-end (org-element-property :contents-end element))
                         (content (if (and contents-begin contents-end)
                                      (buffer-substring-no-properties contents-begin contents-end)
                                    "")))
                    (puthash block-id
                             (list :begin begin
                                   :end end
                                   :file file
                                   :buffer buffer
                                   :content content)
                             org-duckdb-blocks-registry))))))

          (forward-line 1))))

    ;; Second pass: remove stale registry entries
    (let ((blocks-to-remove nil))
      (maphash (lambda (block-id info)
                 (when (and (equal (plist-get info :buffer) buffer)
                            (equal (plist-get info :file) file))
                   (let* ((begin (plist-get info :begin))
                          (end (plist-get info :end))
                          (pos-key (cons begin end))
                          (current-id (gethash pos-key found-blocks)))
                     (when (or (not (member block-id current-buffer-blocks))
                               (and current-id (not (equal current-id block-id))))
                       (push block-id blocks-to-remove)))))
               org-duckdb-blocks-registry)

      (when blocks-to-remove
        (dolist (block-id blocks-to-remove)
          (message "[duckdb-blocks] Removing stale block %s from registry"
                   (substring block-id 0 8))
          (remhash block-id org-duckdb-blocks-registry)))

      current-buffer-blocks)))

;;;; Block Registration
(defun org-duckdb-blocks-register-execution (exec-id session body params is-async-p element)

  "Register execution of DuckDB block at point if tracking enabled.

EXEC-ID is the execution UUID generated by ob-duckdb.el.
SESSION is session name string or nil.
BODY is SQL code being executed (after variable expansion).
PARAMS is alist of header arguments.
IS-ASYNC-P is non-nil if async execution.
ELEMENT is org-element at point (source block).

Only performs registration when `org-duckdb-blocks-enable-tracking'
is non-nil. When disabled, returns nil immediately.

When enabled, this is the core function capturing block state:
1. Identifies source block and extracts properties
2. Updates registry with current positions
3. Assigns block ID (reusing existing or generating new UUID)
4. Uses exec-id from ob-duckdb.el (passed via hook)
5. Records execution details in `org-duckdb-blocks-executions'
6. Updates `org-duckdb-blocks-history-vector'
7. Stores IDs via `org-duckdb-blocks--store-id'

Returns plist with :block-id and :exec-id for ob-duckdb.el, or nil
if tracking disabled.

Called by hook handler when execution starts.

For interactive use, see `org-duckdb-blocks-list' and
`org-duckdb-blocks-recent' to view registered executions."
  (when (and (boundp 'org-duckdb-blocks-enable-tracking)
             org-duckdb-blocks-enable-tracking)
    (let* ((is-duckdb (and (eq (car element) 'src-block)
                           (string= (org-element-property :language element) "duckdb"))))
      (when is-duckdb
        (let* ((begin (org-element-property :begin element))
               (end (org-element-property :end element))
               (contents-begin (org-element-property :contents-begin element))
               (contents-end (org-element-property :contents-end element))
               (content (or (org-element-property :value element)
                            (and contents-begin contents-end
                                 (buffer-substring-no-properties contents-begin contents-end))
                            ""))
               (file (buffer-file-name))
               (buffer (buffer-name))
               (parameters (org-element-property :parameters element))
               (header (org-element-property :header element))
               (switches (org-element-property :switches element))
               (line-count (with-temp-buffer
                             (insert content)
                             (count-lines (point-min) (point-max))))
               (name (org-element-property :name element)))

          ;; Find existing block ID or generate new one
          (let* ((existing-id (org-duckdb-blocks-get-block-id begin))
                 (block-id (org-duckdb-blocks-normalize-id
                            (or existing-id (org-id-uuid))))
                 ;; Use exec-id from ob-duckdb.el (passed via hook)
                 (exec-id (org-duckdb-blocks-normalize-id exec-id))
                 (timestamp (current-time)))

            ;; Store IDs
            (org-duckdb-blocks--store-id block-id exec-id begin)

            ;; Update registry
            (puthash block-id
                     (list :begin begin
                           :end end
                           :file file
                           :buffer buffer
                           :content content)
                     org-duckdb-blocks-registry)

            ;; Record execution
            (puthash exec-id
                     (list :block-id block-id
                           :begin begin
                           :time timestamp
                           :content content
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

            (list :block-id block-id :exec-id exec-id)))))))

;;;; Hook Handlers

(defun org-duckdb-blocks--on-status-changed (exec-id status)
  "Hook handler for status changes.

EXEC-ID and STATUS are from hook.

Updates status via `org-duckdb-blocks-update-execution-status'.

Installed by `org-duckdb-blocks-setup' on
`org-babel-duckdb-status-changed-functions'."
  (org-duckdb-blocks-update-execution-status exec-id status nil))

(defun org-duckdb-blocks--on-execution-started (exec-id session body params is-async-p element)
  "Hook handler for execution start.

EXEC-ID, SESSION, BODY, PARAMS, IS-ASYNC-P, ELEMENT are from hook.

Registers execution via `org-duckdb-blocks-register-execution'.

Installed by `org-duckdb-blocks-setup' on
`org-babel-duckdb-execution-started-functions'."
  (org-duckdb-blocks-register-execution exec-id session body params is-async-p element))

(defun org-duckdb-blocks--on-process-started (exec-id process)
  "Hook handler for async process start.

EXEC-ID and PROCESS are from hook.

Stores process reference via `org-duckdb-blocks-store-process'.

Installed by `org-duckdb-blocks-setup' on
`org-babel-duckdb-async-process-started-functions'."
  (org-duckdb-blocks-store-process exec-id process))

(defun org-duckdb-blocks--on-execution-completed (exec-id status error-info)
  "Hook handler for execution completion.

EXEC-ID, STATUS, ERROR-INFO are from hook.

Updates status via `org-duckdb-blocks-update-execution-status'.

Installed by `org-duckdb-blocks-setup' on
`org-babel-duckdb-execution-completed-functions'."
  (org-duckdb-blocks-update-execution-status exec-id status error-info))

;;;; Navigation Commands
(defun org-duckdb-blocks--find-block-by-id (block-id)
  "Find source block with BLOCK-ID in current buffer.

Scans buffer for #+begin_src duckdb lines with text property
org-duckdb-block-id matching BLOCK-ID.

Returns position of #+begin_src line, or nil if not found.

Used by navigation commands to locate blocks after document edits."
  (let ((block-id (org-duckdb-blocks-normalize-id block-id))
        (found-pos nil))
    (save-excursion
      (goto-char (point-min))
      (while (and (not found-pos)
                  (re-search-forward "^#\\+begin_src duckdb" nil t))
        (let ((line-start (line-beginning-position)))
          (when (equal (org-duckdb-blocks-normalize-id
                       (get-text-property line-start 'org-duckdb-block-id))
                      block-id)
            (setq found-pos line-start)))))
    found-pos))

;;;###autoload
(defun org-duckdb-blocks-goto-block (id)
  "Navigate to DuckDB source block with ID.

Finds block by scanning current buffer for text property
org-duckdb-block-id. Opens file if needed and positions cursor
at block start.

Text properties move with text during edits, so this reliably
locates blocks even after document modifications.

When interactive, provides completion for all known block IDs.

Also see `org-duckdb-blocks-goto-execution' to navigate by execution ID
and `org-duckdb-blocks-navigate-recent' for recent execution browser."
  (interactive
   (list (let ((choice (completing-read "Block ID: "
                                       (hash-table-keys org-duckdb-blocks-registry))))
           (org-duckdb-blocks-normalize-id choice))))

  (when-let* ((info (gethash (org-duckdb-blocks-normalize-id id)
                            org-duckdb-blocks-registry))
              (buffer-name (plist-get info :buffer)))

    ;; Switch to buffer
    (cond
     ((and (plist-get info :file) (file-exists-p (plist-get info :file)))
      (find-file (plist-get info :file)))
     ((get-buffer buffer-name)
      (switch-to-buffer buffer-name))
     (t
      (user-error "[duckdb-blocks] Source buffer %s not found" buffer-name)))

    ;; Find block by scanning for text property
    (if-let ((pos (org-duckdb-blocks--find-block-by-id id)))
        (progn
          (goto-char pos)
          (recenter-top-bottom)

          ;; Update registry with current position
          (let* ((element (org-element-at-point))
                 (begin (org-element-property :begin element))
                 (end (org-element-property :end element))
                 (contents-begin (org-element-property :contents-begin element))
                 (contents-end (org-element-property :contents-end element))
                 (content (if (and contents-begin contents-end)
                              (buffer-substring-no-properties contents-begin contents-end)
                            "")))
            (puthash (org-duckdb-blocks-normalize-id id)
                     (list :begin begin
                           :end end
                           :file (buffer-file-name)
                           :buffer (buffer-name)
                           :content content)
                     org-duckdb-blocks-registry)))

      (user-error "[duckdb-blocks] Block %s not found in buffer %s"
                  (substring id 0 8) buffer-name))))

;;;###autoload
(defun org-duckdb-blocks-goto-execution (exec-id)
  "Navigate to source block of execution EXEC-ID.

Finds execution in `org-duckdb-blocks-executions', extracts block ID,
and delegates to `org-duckdb-blocks-goto-block'.

When interactive, provides completion for all execution IDs.

Also see `org-duckdb-blocks-navigate-recent' for browsing recent
executions with readable labels."
  (interactive
   (list (let ((choice (completing-read "Execution ID: "
                                       (hash-table-keys org-duckdb-blocks-executions))))
           (org-duckdb-blocks-normalize-id choice))))

  (when-let* ((exec-info (gethash (org-duckdb-blocks-normalize-id exec-id)
                                 org-duckdb-blocks-executions))
              (block-id (plist-get exec-info :block-id)))
    (org-duckdb-blocks-goto-block block-id)))

;;;###autoload
(defun org-duckdb-blocks-navigate-recent ()
  "Navigate to recent executions with completion.

Presents list of recent executions with readable labels:
- Timestamp
- Block name or ID
- Parameters (if any)

Retrieves executions via `org-duckdb-blocks-get-recent-history'.
Most convenient way to revisit recent blocks.

Also see `org-duckdb-blocks-recent' for detailed list view."
  (interactive)
  (let* ((recent (org-duckdb-blocks-get-recent-history 30))
         (options '())
         (selected nil))

    (dolist (entry recent)
      (let* ((exec-id (aref entry 0))
             (block-id (aref entry 1))
             (timestamp (aref entry 2))
             (exec-info (gethash (org-duckdb-blocks-normalize-id exec-id)
                                org-duckdb-blocks-executions))
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

;;;; Reporting Commands

;;;###autoload
(defun org-duckdb-blocks-execution-info (exec-id)
  "Display detailed information about execution EXEC-ID.

Shows in help buffer:
- Block identification and location
- Execution timestamp
- Parameters and state at execution time
- Executed content

When interactive, provides completion for all execution IDs.

Also see `org-duckdb-blocks-recent' for chronological list of executions."
  (interactive
   (list (let ((choice (completing-read "Execution ID: "
                                       (hash-table-keys org-duckdb-blocks-executions))))
           (org-duckdb-blocks-normalize-id choice))))
  (when-let* ((exec-info (gethash (org-duckdb-blocks-normalize-id exec-id)
                                 org-duckdb-blocks-executions))
              (block-id (plist-get exec-info :block-id))
              (block-info (gethash (org-duckdb-blocks-normalize-id block-id)
                                  org-duckdb-blocks-registry)))
    (with-help-window "*DuckDB Execution Info*"
      (princ (format "DuckDB Execution: %s\n" exec-id))
      (princ "==========================\n\n")

      (princ (format "Block ID: %s\n" block-id))
      (princ (format "File: %s\n" (or (plist-get block-info :file) "N/A")))
      (princ (format "Buffer: %s\n" (or (plist-get block-info :buffer) "N/A")))
      (when-let ((name (plist-get exec-info :name)))
        (princ (format "Named Block: %s\n" name)))

      (princ (format "\nExecution Time: %s\n\n"
                     (format-time-string "%Y-%m-%d %H:%M:%S.%3N"
                                        (plist-get exec-info :time))))

      (princ "=== SOURCE BLOCK STATE ===\n")
      (when-let ((params (plist-get exec-info :parameters)))
        (princ (format "Parameters: %s\n" params)))
      (when-let ((header (plist-get exec-info :header)))
        (princ (format "Header Arguments: %S\n" header)))
      (when-let ((switches (plist-get exec-info :switches)))
        (princ (format "Switches: %s\n" switches)))
      (princ (format "Line Count: %d\n\n"
                    (or (plist-get exec-info :line-count) 0)))

      (let ((content (plist-get exec-info :content)))
        (princ "=== CONTENT ===\n")
        (if (and content (not (string-empty-p content)))
            (princ content)
          (princ "[No content available]"))
        (princ "\n")))))

;;;###autoload
(defun org-duckdb-blocks-list (&optional limit)
  "Display tracked blocks and their executions, up to LIMIT per block.

Shows for each block in `org-duckdb-blocks-registry':
- Block ID and location
- Execution history with timestamps
- Parameters used

LIMIT defaults to 10 executions per block.
Use prefix argument to specify different limit.

Also see `org-duckdb-blocks-recent' for chronological view across all blocks."
  (interactive "P")
  (let ((limit (or limit 10)))
    (with-help-window "*DuckDB Blocks*"
      (princ "DuckDB Blocks and Executions\n==========================\n\n")

      (if (zerop (hash-table-count org-duckdb-blocks-registry))
          (princ "No blocks found.\n")

        (maphash
         (lambda (block-id info)
           (princ (format "Block ID: %s\n" block-id))
           (princ (format "  File: %s\n" (or (plist-get info :file) "N/A")))
           (princ (format "  Buffer: %s\n" (or (plist-get info :buffer) "N/A")))
           (princ (format "  Source block: %d-%d\n"
                          (plist-get info :begin)
                          (plist-get info :end)))

           (let ((executions '()))
             (maphash (lambda (exec-id exec-info)
                        (when (equal (org-duckdb-blocks-normalize-id block-id)
                                    (org-duckdb-blocks-normalize-id (plist-get exec-info :block-id)))
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

;;;###autoload
(defun org-duckdb-blocks-recent (&optional limit)
  "Display recent executions chronologically, up to LIMIT entries.

Shows for each execution:
- Timestamp
- Block ID or name
- File and buffer location
- Parameters

Provides time-based view across all files.
LIMIT defaults to 10 executions.

Retrieved via `org-duckdb-blocks-get-recent-history'.
Also see `org-duckdb-blocks-navigate-recent' for interactive navigation."
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
                   (exec-info (gethash (org-duckdb-blocks-normalize-id exec-id)
                                      org-duckdb-blocks-executions))
                   (block-info (gethash (org-duckdb-blocks-normalize-id block-id)
                                       org-duckdb-blocks-registry)))

              (princ (format "[%s] Block: %s\n"
                             (format-time-string "%Y-%m-%d %H:%M:%S.%3N" timestamp)
                             (or (plist-get exec-info :name)
                                 (substring (org-duckdb-blocks-normalize-id block-id) 0 12))))
              (princ (format "    Execution: %s\n" exec-id))

              (when block-info
                (princ (format "    File: %s\n" (or (plist-get block-info :file) "N/A")))
                (princ (format "    Buffer: %s\n" (or (plist-get block-info :buffer) "N/A"))))

              (when-let ((params (plist-get exec-info :parameters)))
                (princ (format "    Parameters: %s\n" params)))
              (when-let ((header (plist-get exec-info :header)))
                (princ (format "    Header: %s...\n"
                               (substring (format "%S" header) 0
                                          (min 60 (length (format "%S" header)))))))

              (princ "\n"))))))))

;;;###autoload
(defun org-duckdb-blocks-show-execution-status ()
  "Show status of recent executions with their states.

Displays status for recent executions from
`org-duckdb-blocks-get-recent-history'.
Status values from `org-duckdb-blocks-execution-status':
- running: Query executing
- completed: Finished successfully
- error: Failed with error
- cancelled: User cancelled
- warning: Completed with warnings
- completed-with-errors: Completed with errors

Also shows error info if status is \\='error.

See `org-duckdb-blocks-update-execution-status' for how status is set."
  (interactive)
  (with-help-window "*DuckDB Execution Status*"
    (princ "DuckDB Execution Status\n")
    (princ "======================\n\n")

    (princ "RECENT EXECUTIONS:\n")
    (let ((recent (org-duckdb-blocks-get-recent-history 15)))
      (if (not recent)
          (princ "  None\n")
        (dolist (entry recent)
          (let* ((exec-id (aref entry 0))
                 (block-id (aref entry 1))
                 (timestamp (aref entry 2))
                 (exec-info (gethash exec-id org-duckdb-blocks-executions))
                 ;; Get status from execution-status hash table, not from exec-info plist
                 (status (org-duckdb-blocks-get-execution-status exec-id))
                 (error-info (plist-get exec-info :error-info))
                 (time-str (format-time-string "%H:%M:%S" timestamp)))
            (princ (format "  [%s] %s: %s\n"
                           time-str
                           (substring exec-id 0 12)
                           (or status "unknown")))
            (when error-info
              (princ (format "           Error: %s\n"
                             (if (> (length error-info) 60)
                                 (concat (substring error-info 0 57) "...")
                               error-info))))))))))

;;;; System Setup and Utilities

;;;###autoload
(defun org-duckdb-blocks-setup ()
  "Initialize DuckDB block tracking system if enabled.

Only activates when `org-duckdb-blocks-enable-tracking' is non-nil.

When enabled, adds hook handlers to observe ob-duckdb.el execution:
- `org-babel-duckdb-execution-started-functions'
- `org-babel-duckdb-async-process-started-functions'
- `org-babel-duckdb-execution-completed-functions'
- `org-babel-duckdb-status-changed-functions'

Also excludes tracking properties from yank to prevent ID pollution
when copying source blocks.

Safe to call multiple times (checks if hooks already present).

When disabled, block tracking remains inactive and ob-duckdb.el uses
minimal state for async routing only.

To enable full tracking:
  (setq org-duckdb-blocks-enable-tracking t)
  (org-duckdb-blocks-setup)

Main entry point for using this package."
  (interactive)
  (if (and (boundp 'org-duckdb-blocks-enable-tracking)
           org-duckdb-blocks-enable-tracking)
      (progn
        ;; Install hook handlers
        (unless (member 'org-duckdb-blocks--on-execution-started
                       org-babel-duckdb-execution-started-functions)
          (add-hook 'org-babel-duckdb-execution-started-functions
                    #'org-duckdb-blocks--on-execution-started))

        (unless (member 'org-duckdb-blocks--on-process-started
                       org-babel-duckdb-async-process-started-functions)
          (add-hook 'org-babel-duckdb-async-process-started-functions
                    #'org-duckdb-blocks--on-process-started))

        (unless (member 'org-duckdb-blocks--on-execution-completed
                       org-babel-duckdb-execution-completed-functions)
          (add-hook 'org-babel-duckdb-execution-completed-functions
                    #'org-duckdb-blocks--on-execution-completed))

        (unless (member 'org-duckdb-blocks--on-status-changed
                       org-babel-duckdb-status-changed-functions)
          (add-hook 'org-babel-duckdb-status-changed-functions
                    #'org-duckdb-blocks--on-status-changed))

        ;; Exclude tracking properties from yank
        (dolist (prop '(org-duckdb-block-id org-duckdb-exec-id))
          (unless (memq prop yank-excluded-properties)
            (push prop yank-excluded-properties)))

        (message "DuckDB block tracking activated"))
    (message "DuckDB block tracking disabled (set org-duckdb-blocks-enable-tracking to enable)")))

;;;###autoload
(defun org-duckdb-blocks-clear ()
  "Reset all DuckDB block tracking data.

Clears:
- `org-duckdb-blocks-registry'
- `org-duckdb-blocks-executions'
- `org-duckdb-blocks-execution-status'
- `org-duckdb-blocks-history-vector'

Useful for testing or if tracking data becomes corrupted.
Does not remove #+PROPERTY: lines from source blocks.

To restart tracking, call `org-duckdb-blocks-setup'."
  (interactive)
  (clrhash org-duckdb-blocks-registry)
  (clrhash org-duckdb-blocks-executions)
  (clrhash org-duckdb-blocks-execution-status)
  (setq org-duckdb-blocks-history-vector (make-vector org-duckdb-blocks-history-capacity nil)
        org-duckdb-blocks-history-index 0
        org-duckdb-blocks-history-count 0)
  (message "DuckDB block tracking data cleared"))

(provide 'org-duckdb-blocks)

;;; org-duckdb-blocks.el ends here
