"""Desktop entry point."""

from __future__ import annotations

import tkinter as tk

from app.ui.main_window import MainWindow
from app.utils.file_utils import configure_playwright_runtime


def main() -> None:
    """Start the Tkinter application."""
    configure_playwright_runtime()
    root = tk.Tk()
    MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
