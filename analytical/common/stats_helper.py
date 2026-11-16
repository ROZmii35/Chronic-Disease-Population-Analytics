import numpy as np
from scipy import stats

def wilson_ci(k, n, confidence=0.95):
    """Menghitung Wilson Score Interval 95% untuk proporsi biner k / n."""
    if n == 0:
        return (0.0, 0.0)
    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    p = k / n
    denom = 1 + (z**2) / n
    center = (p + (z**2) / (2 * n)) / denom
    half_width = (z * np.sqrt((p * (1 - p) + (z**2) / (4 * n)) / n)) / denom
    return (float(max(0.0, center - half_width)), float(min(1.0, center + half_width)))

def compute_cramers_v(contingency_matrix):
    """Menghitung Cramér's V untuk matriks kontingensi."""
    chi2, _, _, _ = stats.chi2_contingency(contingency_matrix)
    n = contingency_matrix.sum()
    r, k = contingency_matrix.shape
    min_dim = min(r - 1, k - 1)
    if min_dim == 0 or n == 0:
        return 0.0
    return float(np.sqrt(chi2 / (n * min_dim)))

def cohens_d(x1_mean, x1_std, n1, x2_mean, x2_std, n2):
    """Menghitung Cohen's d antara dua kelompok sampel besar."""
    s_pooled = np.sqrt(((n1 - 1) * (x1_std**2) + (n2 - 1) * (x2_std**2)) / (n1 + n2 - 2))
    if s_pooled == 0:
        return 0.0
    return float((x1_mean - x2_mean) / s_pooled)
