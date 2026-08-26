"""Builds the human-readable diagnostic report (spec section 7) and writes
all per-PDF JSON artifacts (spec section 8)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .analyzer import PdfAnalysis
from ..utils.json_utils import write_json

logger = logging.getLogger(__name__)


def build_text_report(analysis: PdfAnalysis) -> str:
    """Render the plain-text diagnostic report described in spec section 7."""
    lines: list[str] = [f"PDF: {analysis.source_file}", "", f"Pages: {analysis.metadata.page_count}", ""]

    for page in analysis.pages:
        lines.append(f"PAGE {page.page_number}")
        lines.append("-" * 28)
        lines.append("")
        lines.append(f"Words detected: {page.word_count}")
        lines.append(f"Lines detected: {page.line_count}")
        lines.append(f"Rectangles detected: {page.rect_count}")
        lines.append("")

        for strategy_name, tables in page.tables_by_strategy.items():
            lines.append(f"Strategy: {strategy_name}")
            lines.append(f"Tables detected: {len(tables)}")
            for t in tables:
                lines.append(f"  Table {t.table_index}: Rows: {t.n_rows}  Columns: {t.n_cols}")
            lines.append("")

        lines.append(f"BEST CANDIDATE: strategy = {page.best_strategy or 'none'}")
        lines.append("")

        if page.warnings:
            lines.append("Warnings:")
            for w in sorted(set(page.warnings)):
                lines.append(f"- {w}")
        else:
            lines.append("Warnings: none")
        lines.append("")

    return "\n".join(lines)


def _diagnostic_dict(analysis: PdfAnalysis) -> dict[str, Any]:
    return {
        "source_file": analysis.source_file,
        "page_count": analysis.metadata.page_count,
        "pages": [
            {
                "page_number": p.page_number,
                "word_count": p.word_count,
                "line_count": p.line_count,
                "rect_count": p.rect_count,
                "strategies": {
                    name: {
                        "tables_detected": len(tables),
                        "tables": [
                            {
                                "table_index": t.table_index,
                                "rows": t.n_rows,
                                "columns": t.n_cols,
                                "bbox": t.bbox,
                                "warning_count": len(t.warnings),
                            }
                            for t in tables
                        ],
                    }
                    for name, tables in p.tables_by_strategy.items()
                },
                "best_strategy": p.best_strategy,
                "warnings": p.warnings,
            }
            for p in analysis.pages
        ],
    }


def write_pdf_outputs(analysis: PdfAnalysis, output_root: Path) -> Path:
    """Write metadata.json, words.json, tables_*.json, diagnostic.json and a
    plain-text report for one PDF, under output/<pdf_stem>/."""
    pdf_output_dir = output_root / Path(analysis.source_file).stem
    pdf_output_dir.mkdir(parents=True, exist_ok=True)

    write_json(pdf_output_dir / "metadata.json", analysis.metadata)
    write_json(pdf_output_dir / "words.json", analysis.words)
    write_json(pdf_output_dir / "tables_lines.json", analysis.tables_by_strategy.get("lines", []))
    write_json(pdf_output_dir / "tables_text.json", analysis.tables_by_strategy.get("text", []))
    write_json(pdf_output_dir / "tables_combined.json", analysis.tables_by_strategy.get("combined", []))
    write_json(pdf_output_dir / "diagnostic.json", _diagnostic_dict(analysis))

    report_text = build_text_report(analysis)
    (pdf_output_dir / "diagnostic_report.txt").write_text(report_text, encoding="utf-8")

    logger.info("Wrote diagnostic outputs for %s to %s", analysis.source_file, pdf_output_dir)
    return pdf_output_dir


def build_summary(analyses: list[PdfAnalysis]) -> dict[str, Any]:
    """Aggregate statistics across all processed PDFs (spec section 8)."""
    return {
        "pdf_count": len(analyses),
        "pdfs": [
            {
                "source_file": analysis.source_file,
                "page_count": analysis.metadata.page_count,
                "word_count": len(analysis.words),
                "tables_detected_by_strategy": {
                    name: len(tables) for name, tables in analysis.tables_by_strategy.items()
                },
                "warning_count": sum(len(p.warnings) for p in analysis.pages),
                "best_strategy_per_page": [p.best_strategy for p in analysis.pages],
            }
            for analysis in analyses
        ],
    }
