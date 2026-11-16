import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Setup visual style matplotlib untuk video 1080p (Font >= 18pt, rasio 16:9)
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams.update({
    "font.size": 18,
    "axes.titlesize": 22,
    "axes.labelsize": 18,
    "xtick.labelsize": 16,
    "ytick.labelsize": 16,
    "legend.fontsize": 16,
    "figure.titlesize": 24,
    "figure.figsize": (16, 9),  # 16:9 ratio (1920x1080 pada dpi 120)
    "figure.dpi": 120
})

# Palet ramah buta warna (Colorblind-friendly / Wong / Tol palette)
COLORS = {
    "primary": "#0072B2",        # Biru (Okabe-Ito)
    "secondary": "#E69F00",      # Amber / Jingga
    "danger": "#D55E00",         # Vermilion / Oranye Kemerahan
    "success": "#009E73",        # Hijau Kebiruan (Bluish Green)
    "purple": "#CC79A7",         # Merah Muda Keunguan (Reddish Purple)
    "neutral": "#56B4E9",        # Sky Blue
    "dark": "#333333",           # Abu Gelap
    "light_gray": "#D3D3D3",     # Abu Terang
    "readmitted_no": "#009E73",  # Hijau (No Readmission)
    "readmitted_gt30": "#E69F00",# Jingga (>30d)
    "readmitted_lt30": "#D55E00" # Vermilion (<30d)
}

def add_slide_footer(fig, source_text: str):
    """Menambahkan footer standar pada grafik slide: Sumber dan disclaimer data sintetis."""
    fig.text(
        0.05, 0.02,
        f"Sumber: {source_text} | Data sintetis (bootstrap UCI Diabetes)",
        fontsize=12, color="#555555", style="italic"
    )

def save_matplotlib_chart(fig, output_png_path: Path, source_text: str = "encounter_analytics"):
    """Menyimpan figur matplotlib sebagai gambar 1920x1080 untuk slide video."""
    output_png_path.parent.mkdir(parents=True, exist_ok=True)
    add_slide_footer(fig, source_text)
    fig.savefig(output_png_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"[CHART SAVED] {output_png_path}")

def save_plotly_chart(fig, output_html_path: Path):
    """Menyimpan figur Plotly sebagai file HTML interaktif."""
    output_html_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_html_path), include_plotlyjs="cdn")
    print(f"[HTML SAVED] {output_html_path}")
