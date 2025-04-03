;;; ob-duckdb-benchmark.el --- Comprehensive benchmarking for ob-duckdb -*- lexical-binding: t; -*-

;; Author: gggion
;; Keywords: duckdb, sql, org, benchmark
;; Package-Requires: ((emacs "27.1") (ob-duckdb "0.1"))

;; This file is NOT part of GNU Emacs.

;;; Commentary:
;;
;; This file provides a comprehensive benchmarking suite for the ob-duckdb package.
;; It tests various aspects of the package's performance, including:
;;
;; - Variable handling (conversion from Elisp to DuckDB)
;; - Output cleaning and formatting
;; - Table vs. scalar detection
;; - String manipulation strategies
;; - Collection handling approaches
;; - Session management performance
;; - Output buffer handling
;;
;; Usage:
;; 1. Load the file: (load-file "path/to/ob-duckdb-benchmark.el")
;; 2. Run individual benchmarks with M-x ob-duckdb-benchmark-*
;; 3. Run all benchmarks with M-x ob-duckdb-run-all-benchmarks
;; 4. Compare optimized implementations with M-x ob-duckdb-benchmark-before-after-optimization

;;; Code:

(require 'ob-duckdb)
(require 'cl-lib)
(require 'benchmark)
(require 'cl)
(require 'org)
;;; Benchmark utilities
;;;
;;; These are adapted from alphapapa's Emacs Development Handbook
;;; https://github.com/alphapapa/emacs-package-dev-handbook

;; Basic benchmark macro
;;;###autoload
(cl-defmacro bench (&optional (times 100000) &rest body)
  "Call `benchmark-run-compiled' on BODY with TIMES iterations.
Returns result as a list suitable for Org tables.
Garbage is collected before running to avoid counting existing garbage."
  (declare (indent defun))
  `(progn
     (garbage-collect)
     (list '("Total runtime" "# of GCs" "Total GC runtime")
           'hline
           (benchmark-run-compiled ,times
             (progn
               ,@body)))))

;; Multi-form benchmark macro
;;;###autoload
(cl-defmacro bench-multi (&key (times 1) forms ensure-equal raw)
  "Return Org table with benchmark results for FORMS.
Runs each form in FORMS with `benchmark-run-compiled' for TIMES iterations.

When ENSURE-EQUAL is non-nil, the results of FORMS are compared
and an error is raised if they're not `equal'.

The first element of each form can be a string description.

Each form is run after garbage collection to ensure clean results."
  (declare (indent defun))
  (let* ((keys (gensym "keys"))
         (result-times (gensym "result-times"))
         (header '(("Form" "x fastest" "Total runtime" "# of GCs" "Total GC runtime")
                   hline))
         ;; Copy forms so that a subsequent call of the macro will get the original forms.
         (forms (cl-copy-list forms))
         (descriptions (cl-loop for form in forms
                                for i from 0
                                collect (if (stringp (car form))
                                            (prog1 (car form)
                                              (setf (nth i forms) (cadr (nth i forms))))
                                          i))))
    `(unwind-protect
         (progn
           (defvar bench-multi-results nil)
           (let* ((bench-multi-results (make-hash-table))
                  (,result-times (sort (list ,@(cl-loop for form in forms
                                                        for i from 0
                                                        for description = (nth i descriptions)
                                                        collect `(progn
                                                                   (garbage-collect)
                                                                   (cons ,description
                                                                         (benchmark-run-compiled ,times
                                                                           ,(if ensure-equal
                                                                                `(puthash ,description ,form bench-multi-results)
                                                                              form))))))
                                       (lambda (a b)
                                         (< (second a) (second b))))))
             ,(when ensure-equal
                `(cl-loop with ,keys = (hash-table-keys bench-multi-results)
                          for i from 0 to (- (length ,keys) 2)
                          unless (equal (gethash (nth i ,keys) bench-multi-results)
                                        (gethash (nth (1+ i) ,keys) bench-multi-results))
                          do (if (sequencep (gethash (car (hash-table-keys bench-multi-results)) bench-multi-results))
                                 (let* ((k1) (k2)
                                        ;; If the difference in one order is nil, try in other order.
                                        (difference (or (setq k1 (nth i ,keys)
                                                              k2 (nth (1+ i) ,keys)
                                                              difference (seq-difference (gethash k1 bench-multi-results)
                                                                                         (gethash k2 bench-multi-results)))
                                                        (setq k1 (nth (1+ i) ,keys)
                                                              k2 (nth i ,keys)
                                                              difference (seq-difference (gethash k1 bench-multi-results)
                                                                                         (gethash k2 bench-multi-results))))))
                                   (user-error "Forms' results not equal: difference (%s - %s): %S"
                                               k1 k2 difference))
                               ;; Not a sequence
                               (user-error "Forms' results not equal: %s:%S %s:%S"
                                           (nth i ,keys) (nth (1+ i) ,keys)
                                           (gethash (nth i ,keys) bench-multi-results)
                                           (gethash (nth (1+ i) ,keys) bench-multi-results)))))
             ;; Add factors to times and return table
             (if ,raw
                 ,result-times
               (append ',header
                       (bench-multi-process-results ,result-times)))))
       (unintern 'bench-multi-results nil))))

(defun bench-multi-process-results (results)
  "Return sorted RESULTS with factors added."
  (setq results (sort results (lambda (a b) (< (second a) (second b)))))
  (cl-loop with length = (length results)
           for i from 0 below length
           for description = (car (nth i results))
           for factor = (pcase i
                          (0 "fastest")
                          (_ (format "%.2f" (/ (second (nth i results))
                                               (second (nth 0 results))))))
           collect (append (list description factor)
                           (list (format "%.6f" (second (nth i results)))
                                 (third (nth i results))
                                 (if (> (fourth (nth i results)) 0)
                                     (format "%.6f" (fourth (nth i results)))
                                   0)))))

;; Multi-form benchmark with lexical binding
;;;###autoload
(cl-defmacro bench-multi-lexical (&key (times 1) forms ensure-equal raw)
  "Return Org table with benchmark results for FORMS with lexical binding enabled.
Creates and byte-compiles a temp file with `lexical-binding: t` to run the benchmark.
See `bench-multi' for details on parameters."
  (declare (indent defun))
  `(let* ((temp-file (concat (make-temp-file "bench-multi-lexical-") ".el"))
          (fn (gensym "bench-multi-lexical-run-")))
     (with-temp-file temp-file
       (insert ";; -*- lexical-binding: t; -*-" "\n\n"
               "(defvar bench-multi-results)" "\n\n"
               (format "(defun %s () (bench-multi :times %d :ensure-equal %s :raw %s :forms %S))"
                       fn ,times ,ensure-equal ,raw ',forms)))
     (unwind-protect
         (if (byte-compile-file temp-file 'load)
             (funcall (intern (symbol-name fn)))
           (user-error "Error byte-compiling and loading temp file"))
       (delete-file temp-file)
       (unintern (symbol-name fn) nil))))

;; Dynamic vs lexical binding comparison
;;;###autoload
(cl-defmacro bench-dynamic-vs-lexical-binding (&key (times 1) forms ensure-equal)
  "Compare FORMS with both dynamic and lexical binding.
Calls both `bench-multi' and `bench-multi-lexical', merges results, and returns
a table showing relative performance of each approach."
  (declare (indent defun))
  `(let ((dynamic (bench-multi :times ,times :ensure-equal ,ensure-equal :raw t
                    :forms ,forms))
         (lexical (bench-multi-lexical :times ,times :ensure-equal ,ensure-equal :raw t
                    :forms ,forms))
         (header '("Form" "x fastest" "Total runtime" "# of GCs" "Total GC runtime")))
     (cl-loop for result in-ref dynamic
              do (setf (car result) (format "Dynamic: %s" (car result))))
     (cl-loop for result in-ref lexical
              do (setf (car result) (format "Lexical: %s" (car result))))
     (append (list header)
             (list 'hline)
             (bench-multi-process-results (append dynamic lexical)))))

;;; Test fixtures

(defvar ob-duckdb-benchmark-fixture-small
  "SELECT 1 as num, 'test' as str;"
  "A small query for benchmarking.")

(defvar ob-duckdb-benchmark-fixture-medium
  "WITH RECURSIVE numbers(n) AS
   (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 100)
   SELECT n, n*2 as double, 'text-' || n as text FROM numbers;"
  "A medium-sized query for benchmarking.")

(defvar ob-duckdb-benchmark-fixture-large
  "WITH RECURSIVE numbers(n) AS
   (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 1000)
   SELECT n, n*2 as double, 'text-' || n as text FROM numbers;"
  "A large query for benchmarking.")

(defvar ob-duckdb-benchmark-params-simple
  '((:var x 10) (:var y "test"))
  "Simple parameters for benchmarking variable substitution.")

(defvar ob-duckdb-benchmark-params-complex
  '((:var x ((1 "a") (2 "b") (3 "c")))
    (:var y (1 2 3 4 5)))
  "Complex parameters for benchmarking variable substitution.")

(defvar ob-duckdb-benchmark-output-sample
  "D Version v0.8.1
D · SELECT 1 as test;
┌──────┐
│ test │
├──────┤
│    1 │
└──────┘
"
  "Sample DuckDB output for benchmarking cleanup functions.")

(defvar ob-duckdb-benchmark-progress-sample
  "D Loading table... 25% ▕████                             ▏
D Loading table... 50% ▕████████                         ▏
D Loading table... 75% ▕████████████                     ▏
D Loading table... 100% ▕████████████████████████████████▏
D · SELECT count(*) FROM large_table;
┌──────────┐
│ count(*) │
├──────────┤
│    10000 │
└──────────┘
"
  "Sample DuckDB output with progress bars for benchmarking cleanup functions.")

(defvar ob-duckdb-benchmark-table-output
  "┌───┬───────┐
│ n │ name  │
├───┼───────┤
│ 1 │ Alice │
│ 2 │ Bob   │
└───┴───────┘"
  "Sample table output for benchmarking.")

(defvar ob-duckdb-benchmark-scalar-output
  "┌───────────┐
│ count(*) │
├───────────┤
│       42 │
└───────────┘"
  "Sample scalar output for benchmarking.")

;;; Core Function Benchmarks

(defun ob-duckdb-benchmark-var-to-duckdb ()
  "Benchmark different data type conversions with org-babel-duckdb-var-to-duckdb."
  (interactive)
  (bench-multi :times 10000
    :forms (("nil value"               (org-babel-duckdb-var-to-duckdb nil))
            ("String conversion"       (org-babel-duckdb-var-to-duckdb "test string"))
            ("Number conversion"       (org-babel-duckdb-var-to-duckdb 42))
            ("Simple list conversion"  (org-babel-duckdb-var-to-duckdb '(1 2 3 4 5)))
            ("Nested list conversion"  (org-babel-duckdb-var-to-duckdb '((1 "a") (2 "b") (3 "c"))))
            ("Table with header"       (org-babel-duckdb-var-to-duckdb '(("col1" "col2") (1 "a") (2 "b") (3 "c")))))))

(defun ob-duckdb-benchmark-clean-output ()
  "Benchmark different approaches to cleaning DuckDB output."
  (interactive)
  (bench-multi-lexical :times 1000
    :forms (("Single regex pass"
             (replace-regexp-in-string
              "\\(?:^D[ ·]+\\|^Process duckdb finished$\\|^Use \"\\.open FILENAME\".*\n\\|^ *[0-9]+% ▕[█ ]+▏.*$\\)"
              ""
              ob-duckdb-benchmark-output-sample))
            ("String processing approach"
             (let* ((lines (split-string ob-duckdb-benchmark-output-sample "\n" t))
                    (filtered-lines
                     (cl-remove-if
                      (lambda (line)
                        (or (string-match-p "^D[ ·]+" line)
                            (string-match-p "^Process duckdb finished$" line)
                            (string-match-p "^Use \"\\.open FILENAME\"" line)
                            (string-match-p "^ *[0-9]+% ▕[█ ]+▏" line)))
                      lines)))
               (mapconcat #'identity filtered-lines "\n")))
            ("Current approach"
             (org-babel-duckdb-clean-output ob-duckdb-benchmark-output-sample))
            ("Current approach with progress bars"
             (org-babel-duckdb-clean-output ob-duckdb-benchmark-progress-sample)))))

(defun ob-duckdb-benchmark-table-or-scalar ()
  "Benchmark table vs scalar detection and conversion."
  (interactive)
  (bench-multi-lexical :times 1000
    :forms (("Scalar detection and parsing"
             (org-babel-duckdb-table-or-scalar ob-duckdb-benchmark-scalar-output))
            ("Table detection and parsing"
             (org-babel-duckdb-table-or-scalar ob-duckdb-benchmark-table-output))
            ("Alternative table parsing"
             (let* ((lines (split-string ob-duckdb-benchmark-table-output "\n" t))
                    (header-line (car lines))
                    (separator-line (nth 1 lines))
                    (data-lines (cddr lines)))
               ;; Drop the first and last line if they contain box-drawing chars
               (when (string-match-p "[┌┐]" (car data-lines))
                 (setq data-lines (cdr data-lines)))
               (when (and data-lines (string-match-p "[└┘]" (car (last data-lines))))
                 (setq data-lines (butlast data-lines)))

               (cons (split-string header-line "│" t)
                     (mapcar (lambda (line)
                               (split-string line "│" t))
                             data-lines)))))))

(defun ob-duckdb-benchmark-binding-comparison ()
  "Compare dynamic vs lexical binding for key functions."
  (interactive)
  (bench-dynamic-vs-lexical-binding :times 1000
    :forms (("Clean output"
             (org-babel-duckdb-clean-output ob-duckdb-benchmark-output-sample))
            ("Variable to DuckDB"
             (org-babel-duckdb-var-to-duckdb '((1 "a") (2 "b"))))
            ("Table or scalar"
             (org-babel-duckdb-table-or-scalar ob-duckdb-benchmark-table-output)))))

;;; Collection Handling Benchmarks

(defun ob-duckdb-benchmark-variable-collection ()
  "Benchmark different ways of collecting items into lists in variable handling."
  (interactive)
  (let ((table-data '(("col1" "col2") ("val1" "val2") ("val3" "val4"))))
    (bench-multi-lexical :times 10000
      :forms (("cl-loop collect"
               (cl-loop for row in table-data
                        collect (cl-loop for val in row
                                         collect (if (stringp val)
                                                     (format "'%s'" val)
                                                   (format "%s" val)))))
              ("push-nreverse"
               (let (result)
                 (dolist (row table-data)
                   (let (row-result)
                     (dolist (val row)
                       (push (if (stringp val)
                                 (format "'%s'" val)
                               (format "%s" val))
                             row-result))
                     (push (nreverse row-result) result)))
                 (nreverse result)))
              ("mapcar with mapcar"
               (mapcar (lambda (row)
                         (mapcar (lambda (val)
                                   (if (stringp val)
                                       (format "'%s'" val)
                                     (format "%s" val)))
                                 row))
                       table-data))))))

(defun ob-duckdb-benchmark-string-building ()
  "Benchmark different approaches to building SQL strings."
  (interactive)
  (let ((list-data '(1 2 3 4 5)))
    (bench-multi-lexical :times 10000
      :forms (("mapconcat + concat"
               (concat "[" (mapconcat (lambda (x) (format "%s" x)) list-data ", ") "]"))
              ("format with mapconcat"
               (format "[%s]" (mapconcat (lambda (x) (format "%s" x)) list-data ", ")))
              ("with-temp-buffer"
               (with-temp-buffer
                 (insert "[")
                 (let ((first t))
                   (dolist (item list-data)
                     (unless first
                       (insert ", "))
                     (setq first nil)
                     (insert (format "%s" item))))
                 (insert "]")
                 (buffer-string)))
              ("cl-loop with concat"
               (concat "["
                       (cl-loop for x in list-data
                                for i from 0
                                concat (if (> i 0) ", " "")
                                concat (format "%s" x))
                       "]"))))))

(defun ob-duckdb-benchmark-lookups ()
  "Benchmark different approaches to looking up values."
  (interactive)
  (let* ((alist (cl-loop for i from 0 to 50
                         collect (cons (format "session-%d" i) i)))
         (plist (cl-loop for (k . v) in alist
                         append (list (intern k) v)))
         (hash-table (make-hash-table :test 'equal)))
    ;; Fill hash table
    (dolist (pair alist)
      (puthash (car pair) (cdr pair) hash-table))

    (bench-multi-lexical :times 10000
      :forms (("alist-get with equal"
               (alist-get "session-25" alist nil nil #'equal))
              ("alist-get with string="
               (alist-get "session-25" alist nil nil #'string=))
              ("plist-get"
               (plist-get plist (intern "session-25")))
              ("gethash"
               (gethash "session-25" hash-table))))))

(defun ob-duckdb-benchmark-advanced-clean-output ()
  "Benchmark different approaches to cleaning DuckDB output with more variations."
  (interactive)
  (bench-multi-lexical :times 1000
    :forms (("Single regex with replace-regexp-in-string"
             (replace-regexp-in-string
              "\\(?:^D[ ·]+\\|^Process duckdb finished$\\|^Use \"\\.open FILENAME\".*\n\\|^ *[0-9]+% ▕[█ ]+▏.*$\\)"
              ""
              ob-duckdb-benchmark-output-sample))
            ("String-split and filter"
             (mapconcat
              #'identity
              (cl-remove-if
               (lambda (line)
                 (or (string-match-p "^D[ ·]+" line)
                     (string-match-p "^Process duckdb finished$" line)
                     (string-match-p "^Use \"\\.open FILENAME\"" line)
                     (string-match-p "^ *[0-9]+% ▕[█ ]+▏" line)))
               (split-string ob-duckdb-benchmark-output-sample "\n" t))
              "\n"))
            ("Multiple targeted regexps"
             (thread-last ob-duckdb-benchmark-output-sample
               (replace-regexp-in-string "^D[ ·]+" "")
               (replace-regexp-in-string "^Process duckdb finished$" "")
               (replace-regexp-in-string "^Use \"\\.open FILENAME\".*\n" "")
               (replace-regexp-in-string "^ *[0-9]+% ▕[█ ]+▏.*$" "")))
            ("With-temp-buffer approach"
             (with-temp-buffer
               (insert ob-duckdb-benchmark-output-sample)
               (goto-char (point-min))
               (while (re-search-forward "^D[ ·]+" nil t)
                 (replace-match ""))
               (goto-char (point-min))
               (while (re-search-forward "^Process duckdb finished$" nil t)
                 (replace-match ""))
               (goto-char (point-min))
               (while (re-search-forward "^Use \"\\.open FILENAME\".*\n" nil t)
                 (replace-match ""))
               (goto-char (point-min))
               (while (re-search-forward "^ *[0-9]+% ▕[█ ]+▏.*$" nil t)
                 (replace-match ""))
               (buffer-string))))))

(defun ob-duckdb-benchmark-table-detection-refined ()
  "Benchmark refined approaches to detecting and parsing tables in output."
  (interactive)
  (bench-multi-lexical :times 10000
    :forms (("Regexp-based detection"
             (and (string-match-p "│.*│.*\n│.*│" ob-duckdb-benchmark-table-output)
                  (string-match-p "─┼─" ob-duckdb-benchmark-table-output)))
            ("Box-drawing char detection"
             (and (string-match-p "[┌┐│┘└]" ob-duckdb-benchmark-table-output)
                  (> (length (split-string ob-duckdb-benchmark-table-output "\n")) 3)))
            ("Line count heuristic"
             (let* ((lines (split-string ob-duckdb-benchmark-table-output "\n" t))
                    (line-count (length lines)))
               (and (> line-count 3)
                    (string-match-p "│" (nth 1 lines))
                    (string-match-p "├" (nth 2 lines))))))))

(defun ob-duckdb-benchmark-session-management ()
  "Benchmark different approaches to session management."
  (interactive)
  (let* ((session-names (cl-loop for i from 0 to 50
                                  collect (format "session-%d" i)))
         ;; Current approach (hash table)
         (hash-sessions (make-hash-table :test 'equal))
         ;; Alternative (alist)
         (alist-sessions '())
         ;; Alternative (plist)
         (plist-sessions '()))

    ;; Set up test data in different formats
    (dolist (name session-names)
      (puthash name (format "buffer-%s" name) hash-sessions)
      (push (cons name (format "buffer-%s" name)) alist-sessions)
      (setq plist-sessions
            (append plist-sessions (list (intern name) (format "buffer-%s" name)))))

    (bench-multi-lexical :times 50000
      :forms (("Current (hash-table)"
               (gethash "session-25" hash-sessions))
              ("Alist with equal"
               (alist-get "session-25" alist-sessions nil nil #'equal))
              ("Alist with string="
               (alist-get "session-25" alist-sessions nil nil #'string=))
              ("Plist (intern first)"
               (plist-get plist-sessions (intern "session-25")))
              ("Plist (pre-interned)"
               (let ((sym (intern "session-25")))
                 (plist-get plist-sessions sym)))))))

(defun ob-duckdb-benchmark-output-buffer-handling ()
  "Benchmark different approaches to output buffer management."
  (interactive)
  (let ((output "Sample output that would go to a buffer"))
    (bench-multi-lexical :times 1000
      :forms (("Get-buffer-create each time"
               (let ((buf (get-buffer-create "*test-output*")))
                 (with-current-buffer buf
                   (erase-buffer)
                   (insert output))
                 buf))
              ("Get-buffer or create once"
               (let ((buf (or (get-buffer "*test-output*")
                              (generate-new-buffer "*test-output*"))))
                 (with-current-buffer buf
                   (erase-buffer)
                   (insert output))
                 buf))
              ("with-current-buffer get-buffer-create"
               (with-current-buffer (get-buffer-create "*test-output*")
                 (erase-buffer)
                 (insert output)
                 (current-buffer)))))))

;;; Optimized Implementations

(defun org-babel-duckdb-clean-output-optimized (output)
  "Optimized version of output cleaning function using a single regex pass."
  (string-trim
   (replace-regexp-in-string
    "\\(?:^D[ ·]+\\|^Process duckdb finished$\\|^Use \"\\.open FILENAME\".*\n\\|^ *[0-9]+% ▕[█ ]+▏.*$\\)"
    ""
    output)))

(defun org-babel-duckdb-var-to-duckdb-optimized (var)
  "Optimized version of variable conversion with fast paths for common types."
  (cond
   ((null var) "NULL")
   ((stringp var)
    (format "'%s'" (replace-regexp-in-string "'" "''" var)))
   ((numberp var) (format "%s" var))
   ((symbolp var)
    (if (eq var 'hline) "NULL" (format "'%s'" var)))
   ((and (listp var)
         (not (listp (car var)))
         (cl-every #'numberp var))
    (format "[%s]" (mapconcat #'number-to-string var ", ")))
   ((and (listp var)
         (not (listp (car var)))
         (cl-every #'stringp var))
    (format "[%s]"
            (mapconcat
             (lambda (s) (format "'%s'" (replace-regexp-in-string "'" "''" s)))
             var ", ")))
   ((listp var)
    (if (and (equal (length var) 1) (listp (car var)))
        ;; Table with just data - use format + mapconcat for better performance
        (format "VALUES %s"
                (mapconcat
                 (lambda (row)
                   (format "(%s)" (mapconcat #'org-babel-duckdb-var-to-duckdb-optimized
                                            row ", ")))
                 var
                 ", "))
      ;; Mixed list
      (format "[%s]"
              (mapconcat #'org-babel-duckdb-var-to-duckdb-optimized var ", "))))
   (t (format "%s" var))))

(defun org-babel-duckdb-table-or-scalar-optimized (result)
  "Optimized version of table-or-scalar detection and parsing.
This uses string pattern matching rather than buffer operations when possible."
  (let* ((lines (split-string result "\n" t))
         (line-count (length lines)))
    (if (and (> line-count 2)
             (string-match-p "│.*│" (nth 0 lines))
             (string-match-p "[┌┐├┘└]" result))
        ;; This is a table, parse it efficiently
        (let* ((headers (split-string (nth 1 lines) "│" t))
               (data-lines (cl-subseq lines 3 (1- line-count))))
          (cons headers
                (mapcar (lambda (line)
                          (split-string line "│" t))
                        data-lines)))
      ;; Not a table, return as is
      result)))

;;; Comparison Benchmarks

(defun ob-duckdb-benchmark-var-to-duckdb-optimized ()
  "Benchmark the optimized variable conversion function."
  (interactive)
  (bench-multi-lexical :times 10000
    :forms (("Original - number"
             (org-babel-duckdb-var-to-duckdb 42))
            ("Optimized - number"
             (org-babel-duckdb-var-to-duckdb-optimized 42))
            ("Original - string"
             (org-babel-duckdb-var-to-duckdb "test"))
            ("Optimized - string"
             (org-babel-duckdb-var-to-duckdb-optimized "test"))
            ("Original - number list"
             (org-babel-duckdb-var-to-duckdb '(1 2 3 4 5)))
            ("Optimized - number list"
             (org-babel-duckdb-var-to-duckdb-optimized '(1 2 3 4 5)))
            ("Original - string list"
             (org-babel-duckdb-var-to-duckdb '("a" "b" "c")))
            ("Optimized - string list"
             (org-babel-duckdb-var-to-duckdb-optimized '("a" "b" "c")))
            ("Original - mixed list"
             (org-babel-duckdb-var-to-duckdb '(1 "b" 3)))
            ("Optimized - mixed list"
             (org-babel-duckdb-var-to-duckdb-optimized '(1 "b" 3)))
            ("Original - table"
             (org-babel-duckdb-var-to-duckdb '(("a" "b") (1 2) (3 4))))
            ("Optimized - table"
             (org-babel-duckdb-var-to-duckdb-optimized '(("a" "b") (1 2) (3 4)))))))

(defun ob-duckdb-benchmark-before-after-optimization ()
  "Compare performance before and after optimization."
  (interactive)
  (let ((buf (get-buffer-create "*ob-duckdb-optimization-results*")))
    (with-current-buffer buf
      (erase-buffer)
      (org-mode)
      (insert "#+TITLE: ob-duckdb Optimization Results\n\n")

      ;; Cleanup function
      (insert "* Clean Output Function\n\n")
      (let ((result (bench-multi :times 1000
                      :forms (("Original"
                               (org-babel-duckdb-clean-output
                                ob-duckdb-benchmark-progress-sample))
                              ("Optimized"
                               (org-babel-duckdb-clean-output-optimized
                                ob-duckdb-benchmark-progress-sample))))))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      ;; Variable conversion function
      (insert "* Variable Conversion Function\n\n")
      (let ((result (bench-multi :times 10000
                      :forms (("Original - number list"
                               (org-babel-duckdb-var-to-duckdb '(1 2 3 4 5)))
                              ("Optimized - number list"
                               (org-babel-duckdb-var-to-duckdb-optimized '(1 2 3 4 5)))
                              ("Original - table"
                               (org-babel-duckdb-var-to-duckdb '(("a" "b") (1 2))))
                              ("Optimized - table"
                               (org-babel-duckdb-var-to-duckdb-optimized '(("a" "b") (1 2))))))))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      ;; Table or scalar function
      (insert "* Table or Scalar Function\n\n")
      (let ((result (bench-multi :times 1000
                      :forms (("Original"
                               (org-babel-duckdb-table-or-scalar
                                ob-duckdb-benchmark-table-output))
                              ("Optimized"
                               (org-babel-duckdb-table-or-scalar-optimized
                                ob-duckdb-benchmark-table-output))))))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (display-buffer buf))))

(defun ob-duckdb-benchmark-defsubst-comparison ()
  "Compare regular functions vs defsubst for small utility functions."
  (interactive)
  (let ((orig-fn (symbol-function 'org-babel-duckdb-var-to-duckdb))
        (orig-table-fn (symbol-function 'org-babel-duckdb-table-or-scalar)))
    (unwind-protect
        (bench-multi-lexical :times 10000
          :forms (("Original function call"
                   (org-babel-duckdb-var-to-duckdb 42))
                  ("Defsubst version"
                   (progn
                     (defsubst org-babel-duckdb-var-to-duckdb-subst (var)
                       "Convert an Emacs Lisp value VAR to a DuckDB SQL value."
                       (cond
                        ;; Handle tables/lists
                        ((listp var)
                         (if (and (equal (length var) 1) (listp (car var)))
                             ;; This is a table with just data
                             (format "VALUES %s"
                                     (mapconcat
                                      (lambda (row)
                                        (concat "(" (mapconcat #'org-babel-duckdb-var-to-duckdb-subst row ", ") ")"))
                                      var
                                      ", "))
                           ;; This is a list/array
                           (concat "[" (mapconcat #'org-babel-duckdb-var-to-duckdb-subst var ", ") "]")))

                        ;; Handle org table horizontal separators
                        ((eq var 'hline)
                         "NULL")

                        ;; Handle strings (escape single quotes by doubling them)
                        ((stringp var)
                         (format "'%s'" (replace-regexp-in-string "'" "''" var)))

                        ;; Handle nil values
                        ((null var)
                         "NULL")

                        ;; Other values (numbers, etc.) - convert to string
                        (t
                         (format "%s" var))))
                     (org-babel-duckdb-var-to-duckdb-subst 42)))
                  ("Complex data with original"
                   (org-babel-duckdb-var-to-duckdb '((1 "a") (2 "b"))))
                  ("Complex data with defsubst"
                   (org-babel-duckdb-var-to-duckdb-subst '((1 "a") (2 "b"))))))

      ;; Restore the original functions
      (fset 'org-babel-duckdb-var-to-duckdb orig-fn)
      (fset 'org-babel-duckdb-table-or-scalar orig-table-fn))))

;;; Full Benchmark Suite

(defun ob-duckdb-run-all-benchmarks ()
  "Run all benchmarks and display results in a buffer."
  (interactive)
  (let ((buffer (get-buffer-create "*ob-duckdb-benchmarks*")))
    (with-current-buffer buffer
      (erase-buffer)
      (org-mode)
      (insert "#+TITLE: ob-duckdb Benchmark Results\n\n")

      ;; Core functions
      (insert "* Core Functions\n\n")

      (insert "** Variable Conversion\n\n")
      (let ((result (ob-duckdb-benchmark-var-to-duckdb)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Output Cleaning\n\n")
      (let ((result (ob-duckdb-benchmark-clean-output)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Table/Scalar Detection\n\n")
      (let ((result (ob-duckdb-benchmark-table-or-scalar)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Dynamic vs. Lexical Binding\n\n")
      (let ((result (ob-duckdb-benchmark-binding-comparison)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      ;; Collection handling
      (insert "* Collection Handling\n\n")

      (insert "** Variable Collection\n\n")
      (let ((result (ob-duckdb-benchmark-variable-collection)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** String Building\n\n")
      (let ((result (ob-duckdb-benchmark-string-building)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Value Lookups\n\n")
      (let ((result (ob-duckdb-benchmark-lookups)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Session Management\n\n")
      (let ((result (ob-duckdb-benchmark-session-management)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Output Buffer Handling\n\n")
      (let ((result (ob-duckdb-benchmark-output-buffer-handling)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      ;; Advanced benchmarks
      (insert "* Advanced Benchmarks\n\n")

      (insert "** Advanced Output Cleaning\n\n")
      (let ((result (ob-duckdb-benchmark-advanced-clean-output)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Table Detection Refined\n\n")
      (let ((result (ob-duckdb-benchmark-table-detection-refined)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      (insert "** Defsubst Comparison\n\n")
      (let ((result (ob-duckdb-benchmark-defsubst-comparison)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      ;; Optimizations
      (insert "* Optimized Implementations\n\n")

      (insert "** Var-to-DuckDB Optimization\n\n")
      (let ((result (ob-duckdb-benchmark-var-to-duckdb-optimized)))
        (insert "#+BEGIN_SRC org\n")
        (pp result (current-buffer))
        (insert "#+END_SRC\n\n"))

      ;; Display buffer
      (goto-char (point-min))
      (switch-to-buffer buffer))))

(provide 'ob-duckdb-benchmark)

;;; ob-duckdb-benchmark.el ends here
