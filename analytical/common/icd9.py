from pyspark.sql.functions import col, when, trim, split

def map_icd9_code(code: str) -> str:
    """
    Pemetaan ICD-9 Python standar untuk satu nilai kode.
    Digunakan untuk unit testing dan validasi data.
    """
    if code is None:
        return "Unknown"
    code_str = str(code).strip()
    if not code_str or code_str == "?" or code_str.lower() == "nan":
        return "Unknown"
    if code_str.startswith("V") or code_str.startswith("E"):
        return "Other (V/E code)"

    prefix_str = code_str.split(".")[0]
    try:
        val = int(prefix_str)
    except ValueError:
        return "Other"

    if val == 250:
        return "Diabetes"
    elif (390 <= val <= 459) or val == 785:
        return "Circulatory"
    elif (460 <= val <= 519) or val == 786:
        return "Respiratory"
    elif (520 <= val <= 579) or val == 787:
        return "Digestive"
    elif 800 <= val <= 999:
        return "Injury"
    elif 710 <= val <= 739:
        return "Musculoskeletal"
    elif (580 <= val <= 629) or val == 788:
        return "Genitourinary"
    elif 140 <= val <= 239:
        return "Neoplasms"
    else:
        return "Other"


def get_icd9_group_expr(column_name="diag_1"):
    """
    Menghasilkan Column Expression PySpark untuk pengelompokan ICD-9 standar:
    - Circulatory: 390-459 & 785
    - Respiratory: 460-519 & 786
    - Digestive: 520-579 & 787
    - Diabetes: 250.xx
    - Injury: 800-999
    - Musculoskeletal: 710-739
    - Genitourinary: 580-629 & 788
    - Neoplasms: 140-239
    - Other (V/E code): berawalan V atau E
    - Unknown: NULL / Kosong / ?
    - Other: kode numerik lainnya
    """
    c = trim(col(column_name))
    prefix = split(c, "\\.")[0].cast("int")

    return (
        when(c.isNull() | (c == "") | (c == "?"), "Unknown")
        .when(c.startswith("V") | c.startswith("E"), "Other (V/E code)")
        .when(prefix == 250, "Diabetes")
        .when(((prefix >= 390) & (prefix <= 459)) | (prefix == 785), "Circulatory")
        .when(((prefix >= 460) & (prefix <= 519)) | (prefix == 786), "Respiratory")
        .when(((prefix >= 520) & (prefix <= 579)) | (prefix == 787), "Digestive")
        .when((prefix >= 800) & (prefix <= 999), "Injury")
        .when((prefix >= 710) & (prefix <= 739), "Musculoskeletal")
        .when(((prefix >= 580) & (prefix <= 629)) | (prefix == 788), "Genitourinary")
        .when((prefix >= 140) & (prefix <= 239), "Neoplasms")
        .otherwise("Other")
    )


# Unit Tests
def test_icd9_mapping():
    cases = [
        ("410.1", "Circulatory"),
        ("785.51", "Circulatory"),
        ("493.9", "Respiratory"),
        ("786.5", "Respiratory"),
        ("540", "Digestive"),
        ("787.0", "Digestive"),
        ("250.02", "Diabetes"),
        ("820.1", "Injury"),
        ("715", "Musculoskeletal"),
        ("590.8", "Genitourinary"),
        ("788.1", "Genitourinary"),
        ("174.9", "Neoplasms"),
        ("V58.61", "Other (V/E code)"),
        ("E876.0", "Other (V/E code)"),
        (None, "Unknown"),
        ("?", "Unknown"),
        ("272.4", "Other")
    ]
    for raw_val, expected in cases:
        actual = map_icd9_code(raw_val)
        assert actual == expected, f"Failed for {raw_val}: expected {expected}, got {actual}"
    print("[SUCCESS] All 17 ICD-9 unit test cases passed!")


if __name__ == "__main__":
    test_icd9_mapping()
