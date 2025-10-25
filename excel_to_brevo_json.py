#!/usr/bin/env python3
import pandas as pd
import json
import argparse
from pathlib import Path

def convert_excel_to_json(
    excel_path: str,
    output_json: str,
    image_base_url: str = "https://cdn.jsdelivr.net/gh/TVScoundrel/artstory-data-sources@main/product-images/",
    brand_col: str = "Brand",
    product_desc_col: str = "Products",
    article_code_col: str = "Article code ",
    discount_col: str = "Discount %",
    sort_products_by: str = None,
):
    df = pd.read_excel(excel_path)

    def find_col(target_name, fallbacks=None):
        fallbacks = fallbacks or []
        if target_name in df.columns:
            return target_name
        for c in df.columns:
            if str(c).strip().lower() == target_name.strip().lower():
                return c
        for name in fallbacks:
            if name in df.columns:
                return name
            for c in df.columns:
                if str(c).strip().lower() == str(name).strip().lower():
                    return c
        return None

    brand_col = find_col(brand_col)
    if not brand_col:
        raise ValueError("Could not find a 'Brand' column.")

    product_desc_col = find_col(product_desc_col, ["Product", "Product name", "Description"])
    if not product_desc_col:
        for c in df.columns:
            if c != brand_col and df[c].dtype == object:
                product_desc_col = c
                break
        if not product_desc_col:
            raise ValueError("Could not infer a product description column.")

    article_code_col = find_col(article_code_col, ["Article code", "Code", "SKU"])
    discount_col = find_col(discount_col, ["Discount%", "Discount", "DISCOUNT %", "discount %"])

    df[brand_col] = df[brand_col].astype(str).str.strip()

    if discount_col and discount_col in df.columns:
        df[discount_col] = (
            pd.to_numeric(df[discount_col], errors="coerce")
            .round(0)
            .abs()  # remove minus sign
            .astype("Int64")
        )
        df[discount_col] = df[discount_col].where(df[discount_col].notna(), None)
        # Rename the column to "Discount" for JSON output
        df.rename(columns={discount_col: "Discount"}, inplace=True)


    if article_code_col and article_code_col in df.columns and image_base_url:
        def build_image(val):
            if pd.isna(val):
                return None
            v = str(val).strip()
            if v == "":
                return None
            if v.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                return f"{image_base_url}{v}"
            return f"{image_base_url}{v}.jpg"
        df["Image"] = df[article_code_col].apply(build_image)

    sort_key = sort_products_by or product_desc_col

    brands = []
    for brand_name, group in df.groupby(brand_col, dropna=False):
        if sort_key in group.columns:
            group_sorted = group.sort_values(
                by=sort_key,
                key=lambda s: s.str.lower() if s.dtype == object else s,
                na_position="last",
            )
        else:
            group_sorted = group.copy()

        products = group_sorted.drop(columns=[brand_col]).to_dict(orient="records")

        brands.append({
            "name": None if pd.isna(brand_name) else str(brand_name),
            "products": products
        })

    brands_sorted = sorted(brands, key=lambda b: (b["name"] is None, (b["name"] or "").lower()))
    output = {"brands": brands_sorted}

    Path(output_json).parent.mkdir(parents=True, exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Convert Excel catalogue to Brevo-ready JSON.")
    parser.add_argument("--excel", required=True, help="Path to Excel file (e.g., catalogue.xlsx)")
    parser.add_argument("--out", required=True, help="Output JSON path")
    parser.add_argument("--image-base", default="https://cdn.jsdelivr.net/gh/TVScoundrel/artstory-data-sources@main/product-images/", help="Base URL for images (ending with a /)")
    parser.add_argument("--brand-col", default="Brand")
    parser.add_argument("--product-desc-col", default="Products")
    parser.add_argument("--article-code-col", default="Article code ")
    parser.add_argument("--discount-col", default="Discount %")
    parser.add_argument("--sort-products-by", default=None, help="Column to sort products by (defaults to product-desc-col)")
    args = parser.parse_args()

    convert_excel_to_json(
        excel_path=args.excel,
        output_json=args.out,
        image_base_url=args.image_base,
        brand_col=args.brand_col,
        product_desc_col=args.product_desc_col,
        article_code_col=args.article_code_col,
        discount_col=args.discount_col,
        sort_products_by=args.sort_products_by,
    )

if __name__ == "__main__":
    main()
