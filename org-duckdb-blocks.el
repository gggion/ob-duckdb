;;; org-duckdb-blocks.el --- Track DuckDB block executions in Org files -*- lexical-binding: t; -*-

;; Author: gggion
;; Version: 1.0.0
;; Package-Requires: ((emacs "28.1") (org "9.5"))
;; Keywords: org, duckdb, data
;; URL: https://github.com/gggion/ob-duckdb

;;; Commentary:

;; The `org-duckdb-blocks' package provides a tracking system for DuckDB
;; source blocks in Org mode documents. It serves as the foundation
;; for the execution history tracking functionality in `ob-duckdb'.
;;
;; This system assigns unique identifiers to both blocks (persistent) and
;; executions (ephemeral), maintains a comprehensive execution history, and
;; provides navigation tools to revisit any executed block.
;;
;; To use this package, simply require it from `ob-duckdb' or activate it
;; directly with:
;;
;;   (require 'org-duckdb-blocks)
;;   (org-duckdb-blocks-setup)

;;; Code:

(require 'org-element)
(require 'org-macs)
(require 'org-id)

;; --- ID normalization utility -----------------------------
(defun odb/clean-id (id)
  "Return ID as plain string without any text properties."
  (if (stringp id) (substring-no-properties id) id))
;; ----------------------------------------------------------

;;; Core Data Structures

(defvar org-duckdb-blocks-registry (make-hash-table :test 'equal)
  "Registry mapping block IDs to metadata.")

(defvar org-duckdb-blocks-executions (make-hash-table :test 'equal)
  "Execution history mapping execution IDs to details.")

(defvar org-duckdb-blocks-history-vector (make-vector 100 nil)
  "Circular buffer containing recent executions for fast access.")

(defvar org-duckdb-blocks-history-index 0
  "Current position in the history vector's circular buffer.")

(defvar org-duckdb-blocks-history-count 0
  "Number of entries currently stored in the history vector.")

(defvar org-duckdb-blocks-history-capacity 100
  "Maximum number of recent executions to store in the history vector.")

;;; Core Utility Functions

(defun org-duckdb-blocks-add-to-history (exec-id block-id timestamp)
  "Record execution in history buffer for EXEC-ID, BLOCK-ID at TIMESTAMP."
  (let ((entry (vector (odb/clean-id exec-id) (odb/clean-id block-id) timestamp)))
    (aset org-duckdb-blocks-history-vector org-duckdb-blocks-history-index entry)
    (setq org-duckdb-blocks-history-index
          (% (1+ org-duckdb-blocks-history-index) org-duckdb-blocks-history-capacity)
          org-duckdb-blocks-history-count
          (min (1+ org-duckdb-blocks-history-count) org-duckdb-blocks-history-capacity))))

(defun org-duckdb-blocks-get-recent-history (n)
  "Retrieve the N most recent executions from history, newest first."
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

;;; Property Management

(defun org-duckdb-blocks-update-properties (block-id exec-id begin)
  "Update properties for block with BLOCK-ID and EXEC_ID at BEGIN."
  (let ((block-id (odb/clean-id block-id))
        (exec-id  (odb/clean-id exec-id)))
    (save-excursion
      (goto-char begin)

      ;; Update or add ID property
      (if (re-search-backward "^#\\+PROPERTY: ID " (max (- begin 200) (point-min)) t)
          (progn
            (beginning-of-line)
            (kill-line)
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
            (kill-line)
            (insert (format "#+PROPERTY: EXEC_ID %s" exec-id)))
        (goto-char begin)
        (forward-line -1)
        (end-of-line)
        (insert (format "\n#+PROPERTY: EXEC_ID %s" exec-id))))))

;;; Block Registration and Management

(defun org-duckdb-blocks-get-block-id (begin)
  "Get block ID from properties for block at BEGIN, or nil if not found."
  (save-excursion
    (goto-char begin)
    (when (re-search-backward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)" (max (- begin 200) (point-min)) t)
      (odb/clean-id (match-string 1)))))

(defun org-duckdb-blocks-update-all-block-positions ()
  "Update positions of all tracked blocks and clean up stale entries.
This function:
1. Collects all blocks in current buffer with their IDs
2. Updates coordinates for blocks that still have IDs
3. Removes blocks from registry that are no longer in the buffer
4. Identifies and removes duplicate block entries at the same position"
  (let ((file (buffer-file-name))
        (buffer (buffer-name))
        (found-blocks (make-hash-table :test 'equal)) ;; position -> id mapping
        (current-buffer-blocks nil))

    ;; First pass: find blocks with IDs in the current buffer
    (save-excursion
      (goto-char (point-min))
      (while (re-search-forward "^#\\+PROPERTY: ID \\([a-f0-9-]+\\)" nil t)
        (let ((block-id (odb/clean-id (match-string 1))))
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
  "Register execution of the DuckDB block at point."
  (interactive)
  (let* ((el (org-element-at-point))
         (is-duckdb (and (eq (car el) 'src-block)
                         (string= (org-element-property :language el) "duckdb"))))
    (when is-duckdb
      (let* ((begin (org-element-property :begin el))
             (end (org-element-property :end el))
             (contents-begin (org-element-property :contents-begin el))
             (contents-end (org-element-property :contents-end el))
             ;; Try different ways to get content
             (content (or (org-element-property :value el)
                          (and contents-begin contents-end
                               (buffer-substring-no-properties contents-begin contents-end))
                          ""))
             (file (buffer-file-name))
             (buffer (buffer-name))
             (pos-key (cons begin end))

             ;; Capture source block properties for state tracking
             (parameters (org-element-property :parameters el))
             (header (org-element-property :header el))
             (switches (org-element-property :switches el))
             (line-count (with-temp-buffer
                           (insert content)
                           (count-lines (point-min) (point-max))))
             (name (org-element-property :name el)))

        ;; First update positions of all known blocks -- this also cleans up stale blocks
        (org-duckdb-blocks-update-all-block-positions)

        ;; Check if there's a block in the registry at this position
        (let* ((existing-by-position nil)
               (existing-id nil))

          ;; Try to find the block at this position in the registry
          (maphash (lambda (id info)
                     (when (and (not existing-by-position)
                                (equal (plist-get info :begin) begin)
                                (equal (plist-get info :end) end)
                                (equal (plist-get info :buffer) buffer)
                                (equal (plist-get info :file) file))
                       (setq existing-by-position id)))
                   org-duckdb-blocks-registry)

          ;; Get existing ID from properties or use position-matched one
          (setq existing-id (or (org-duckdb-blocks-get-block-id begin)
                                existing-by-position))

          ;; Get final block ID (existing or new), always as bare string
          (let* ((block-id (odb/clean-id (or existing-id (org-id-uuid))))
                 (exec-id  (odb/clean-id (org-id-uuid)))
                 (timestamp (current-time)))
            ;; Store or update block in registry
            (puthash block-id
                     (list :begin begin
                           :end end
                           :file file
                           :buffer buffer
                           :content content)
                     org-duckdb-blocks-registry)

            ;; Store execution details with enhanced state tracking
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

            ;; Add execution to history vector
            (org-duckdb-blocks-add-to-history exec-id block-id timestamp)

            ;; Update block properties in the buffer (ID and EXEC_ID)
            (org-duckdb-blocks-update-properties block-id exec-id begin)

            ;; Return execution info
            (message "[duckdb-blocks] Registered block %s execution %s"
                     (substring block-id 0 8)
                     (substring exec-id 0 8))
            (list :block-id block-id :exec-id exec-id)))))))

(defun org-duckdb-blocks-goto-block (id)
  "Navigate to the DuckDB source block with ID."
  (interactive
   (list (let ((choice (completing-read "Block ID: " (hash-table-keys org-duckdb-blocks-registry))))
           (odb/clean-id choice))))
  (when-let* ((info (gethash (odb/clean-id id) org-duckdb-blocks-registry)))
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
  "Navigate to source block of execution EXEC-ID."
  (interactive
   (list (let ((choice (completing-read "Execution ID: " (hash-table-keys org-duckdb-blocks-executions))))
           (odb/clean-id choice))))
  (when-let* ((exec-info (gethash (odb/clean-id exec-id) org-duckdb-blocks-executions))
              (block-id (plist-get exec-info :block-id)))
    (org-duckdb-blocks-goto-block block-id)))

(defun org-duckdb-blocks-execution-info (exec-id)
  "Display detailed information about the execution with EXEC-ID."
  (interactive
   (list (let ((choice (completing-read "Execution ID: " (hash-table-keys org-duckdb-blocks-executions))))
           (odb/clean-id choice))))
  (when-let* ((exec-info (gethash (odb/clean-id exec-id) org-duckdb-blocks-executions))
              (block-id (plist-get exec-info :block-id))
              (block-info (gethash (odb/clean-id block-id) org-duckdb-blocks-registry)))
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
  "Navigate through recent executions with completion."
  (interactive)
  (let* ((recent (org-duckdb-blocks-get-recent-history 30))
         (options '())
         (selected nil))

    ;; Build options with readable labels
    (dolist (entry recent)
      (let* ((exec-id (aref entry 0))
             (block-id (aref entry 1))
             (timestamp (aref entry 2))
             (exec-info (gethash (odb/clean-id exec-id) org-duckdb-blocks-executions))
             (time-str (format-time-string "%Y-%m-%d %H:%M:%S" timestamp))
             (params (plist-get exec-info :parameters))
             (label (format "[%s] %s %s"
                            time-str
                            (if (plist-get exec-info :name)
                                (format "%s" (plist-get exec-info :name))
                              (format "Block %s" (substring (odb/clean-id block-id) 0 8)))
                            (if params (format " (%s)" params) ""))))
        (push (cons label exec-id) options)))

    (when options
      (setq selected (cdr (assoc (completing-read "Go to: " options nil t) options)))
      (when selected
        (org-duckdb-blocks-goto-execution selected)))))

;;; System Setup and Utilities

(defun org-duckdb-blocks-register-advice (&rest _)
  "Advice function that registers DuckDB block execution."
  (when (org-in-src-block-p)
    (let* ((el (org-element-context))
           ;; Force full element parse to ensure all properties are available
           (el-full (org-element-at-point))
           (is-duckdb (and (eq (car el) 'src-block)
                          (string= (org-element-property :language el) "duckdb"))))
      (when is-duckdb
        (org-duckdb-blocks-register-execution)))))

(defun org-duckdb-blocks-setup ()
  "Initialize the DuckDB block tracking system."
  (interactive)
  (unless (advice-member-p 'org-duckdb-blocks-register-advice 'org-babel-execute-src-block)
    (advice-add 'org-babel-execute-src-block :before #'org-duckdb-blocks-register-advice)
    (message "DuckDB block tracking activated")))

(defun org-duckdb-blocks-clear ()
  "Reset all DuckDB block tracking data structures."
  (interactive)
  (clrhash org-duckdb-blocks-registry)
  (clrhash org-duckdb-blocks-executions)
  (setq org-duckdb-blocks-history-vector (make-vector org-duckdb-blocks-history-capacity nil)
        org-duckdb-blocks-history-index 0
        org-duckdb-blocks-history-count 0)
  (message "DuckDB block tracking data cleared"))

;;; Reporting and Visualization

(defun org-duckdb-blocks-list (&optional limit)
  "Display tracked DuckDB blocks and their executions, up to LIMIT per block."
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
                        (when (equal (odb/clean-id block-id) (odb/clean-id (plist-get exec-info :block-id)))
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
  "Display recent DuckDB executions chronologically, up to LIMIT entries."
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
                   (exec-info (gethash (odb/clean-id exec-id) org-duckdb-blocks-executions))
                   (block-info (gethash (odb/clean-id block-id) org-duckdb-blocks-registry)))

              (princ (format "[%s] Block: %s\n"
                             (format-time-string "%Y-%m-%d %H:%M:%S.%3N" timestamp)
                             (or (plist-get exec-info :name)
                                 (substring (odb/clean-id block-id) 0 12))))
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
