import csv
import json
import os

from pathlib import Path
from typing import List, Dict, Any, Optional

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QFileDialog,
    QMessageBox,
    QComboBox,
    QLineEdit,
    QGroupBox,
    QFormLayout,
)


def csv_to_json(csv_path: Path, json_path: Path) -> int:
    """Convert CSV (with headers) to JSON array of objects. Returns row count."""
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows: List[Dict[str, Any]] = [dict(r) for r in reader]

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    return len(rows)


def json_to_csv(json_path: Path, csv_path: Path) -> int:
    """Convert JSON array of objects to CSV. Returns row count."""
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Allow a common wrapper pattern: {"items":[...]}
    if isinstance(data, dict) and "items" in data:
        data = data["items"]

    if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
        raise ValueError("JSON must be a list of objects (or a dict with an 'items' list of objects).")

    rows: List[Dict[str, Any]] = data if data else []
    # Collect union of keys across all rows (stable order: first row keys, then unseen keys)
    fieldnames: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    return len(rows)


class ConverterGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV ⇄ JSON Converter (PyQt6)")
        self.setMinimumWidth(700)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["CSV → JSON", "JSON → CSV"])
        self.mode_combo.currentIndexChanged.connect(self._on_mode_change)

        self.input_path = QLineEdit()
        self.output_path = QLineEdit()
        self.input_path.setPlaceholderText("Select input file…")
        self.output_path.setPlaceholderText("Select output file…")

        self.btn_browse_input = QPushButton("Browse…")
        self.btn_browse_output = QPushButton("Browse…")
        self.btn_run = QPushButton("Convert")
        self.btn_swap = QPushButton("Swap Input/Output")

        self.btn_browse_input.clicked.connect(self._browse_input)
        self.btn_browse_output.clicked.connect(self._browse_output)
        self.btn_run.clicked.connect(self._run)
        self.btn_swap.clicked.connect(self._swap)

        self.status = QLabel("Ready.")
        self.status.setWordWrap(True)
        self.status.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        self._build_layout()
        self._on_mode_change()

    def _build_layout(self):
        root = QVBoxLayout()
        self.setLayout(root)

        # Mode
        mode_box = QGroupBox("Mode")
        mode_layout = QHBoxLayout()
        mode_box.setLayout(mode_layout)
        mode_layout.addWidget(QLabel("Conversion:"))
        mode_layout.addWidget(self.mode_combo)
        mode_layout.addStretch(1)
        root.addWidget(mode_box)

        # Paths
        paths_box = QGroupBox("Files")
        form = QFormLayout()
        paths_box.setLayout(form)

        in_row = QHBoxLayout()
        in_row.addWidget(self.input_path)
        in_row.addWidget(self.btn_browse_input)

        out_row = QHBoxLayout()
        out_row.addWidget(self.output_path)
        out_row.addWidget(self.btn_browse_output)

        form.addRow("Input:", in_row)
        form.addRow("Output:", out_row)
        root.addWidget(paths_box)

        # Actions
        actions = QHBoxLayout()
        actions.addWidget(self.btn_swap)
        actions.addStretch(1)
        actions.addWidget(self.btn_run)
        root.addLayout(actions)

        # Status
        status_box = QGroupBox("Status")
        status_layout = QVBoxLayout()
        status_box.setLayout(status_layout)
        status_layout.addWidget(self.status)
        root.addWidget(status_box)

    def _on_mode_change(self):
        mode = self.mode_combo.currentText()
        if "CSV" in mode:
            self.btn_run.setText("Convert CSV → JSON")
            self.input_path.setPlaceholderText("Select input .csv file…")
            self.output_path.setPlaceholderText("Select output .json file…")
        else:
            self.btn_run.setText("Convert JSON → CSV")
            self.input_path.setPlaceholderText("Select input .json file…")
            self.output_path.setPlaceholderText("Select output .csv file…")

        # If extensions are mismatched, clear output suggestion (optional)
        self.status.setText("Ready.")

    def _browse_input(self):
        mode = self.mode_combo.currentText()
        if mode == "CSV → JSON":
            file_path, _ = QFileDialog.getOpenFileName(self, "Select CSV file", "", "CSV Files (*.csv);;All Files (*)")
        else:
            file_path, _ = QFileDialog.getOpenFileName(self, "Select JSON file", "", "JSON Files (*.json);;All Files (*)")

        if file_path:
            self.input_path.setText(file_path)
            self._suggest_output(file_path)

    def _browse_output(self):
        mode = self.mode_combo.currentText()
        if mode == "CSV → JSON":
            file_path, _ = QFileDialog.getSaveFileName(self, "Save JSON as…", "", "JSON Files (*.json);;All Files (*)")
        else:
            file_path, _ = QFileDialog.getSaveFileName(self, "Save CSV as…", "", "CSV Files (*.csv);;All Files (*)")

        if file_path:
            self.output_path.setText(file_path)

    def _suggest_output(self, input_file: str):
        """Suggest output filename based on input path if output is empty."""
        if self.output_path.text().strip():
            return

        p = Path(input_file)
        mode = self.mode_combo.currentText()
        if mode == "CSV → JSON":
            suggested = p.with_suffix(".json")
        else:
            suggested = p.with_suffix(".csv")
        self.output_path.setText(str(suggested))

    def _swap(self):
        a = self.input_path.text()
        b = self.output_path.text()
        self.input_path.setText(b)
        self.output_path.setText(a)

    def _run(self):
        mode = self.mode_combo.currentText()
        in_text = self.input_path.text().strip()
        out_text = self.output_path.text().strip()

        if not in_text or not out_text:
            QMessageBox.warning(self, "Missing file(s)", "Please select BOTH an input file and an output file.")
            return

        in_path = Path(in_text)
        out_path = Path(out_text)

        if not in_path.exists():
            QMessageBox.critical(self, "Input not found", f"Input file does not exist:\n{in_path}")
            return

        # Basic extension validation (not strict; just helpful)
        if mode == "CSV → JSON":
            if in_path.suffix.lower() != ".csv":
                if not self._confirm_continue("Input file does not end in .csv. Continue anyway?"):
                    return
            if out_path.suffix.lower() != ".json":
                if not self._confirm_continue("Output file does not end in .json. Continue anyway?"):
                    return
        else:
            if in_path.suffix.lower() != ".json":
                if not self._confirm_continue("Input file does not end in .json. Continue anyway?"):
                    return
            if out_path.suffix.lower() != ".csv":
                if not self._confirm_continue("Output file does not end in .csv. Continue anyway?"):
                    return

        # Ensure output directory exists
        out_dir = out_path.parent
        if not out_dir.exists():
            try:
                out_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                QMessageBox.critical(self, "Cannot create folder", f"Failed to create output folder:\n{out_dir}\n\n{e}")
                return

        # Convert
        try:
            if mode == "CSV → JSON":
                count = csv_to_json(in_path, out_path)
                msg = f"✅ Converted {count} row(s)\nCSV: {in_path}\nJSON: {out_path}"
            else:
                count = json_to_csv(in_path, out_path)
                msg = f"✅ Converted {count} row(s)\nJSON: {in_path}\nCSV: {out_path}"

            self.status.setText(msg)
            QMessageBox.information(self, "Success", msg)

        except Exception as e:
            err = f"❌ Conversion failed.\n\n{e}"
            self.status.setText(err)
            QMessageBox.critical(self, "Error", err)

    def _confirm_continue(self, message: str) -> bool:
        resp = QMessageBox.question(
            self,
            "Confirm",
            message,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        return resp == QMessageBox.StandardButton.Yes


def main():
    app = QApplication([])
    win = ConverterGUI()
    win.show()
    app.exec()


if __name__ == "__main__":
    main()
