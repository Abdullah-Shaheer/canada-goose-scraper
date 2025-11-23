from curl_cffi import requests
import json
import re
import os
import pprint
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time

headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Authorization': Your Bearer Authentication token,
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    'correlation-id': 'f4c39df7-4824-4a52-a2fd-e152827c6e6e',
    'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sfdc_dwsid': You can get by mimicking the headers,
    'x-kpsdk-ct': You can get by mimicking the headers,
    'x-kpsdk-h': You can get by mimicking the headers,
    'x-kpsdk-v': You can get by mimicking the headers,
    'x-mobify': You can get by mimicking the headers,
}



def get_id_and_category(cat_id):
    def get_leaf_categories(cat_id, headers, params, depth=0):
        indent = "  " * depth
        print(f"{indent}Fetching category ID: {cat_id}")
        
        url = f'https://www.canadagoose.com/mobify/proxy/api/product/shopper-products/v1/organizations/f_ecom_aata_prd/categories/{cat_id}'
        try:
            resp = requests.get(url, params=params, headers=headers)
            resp.raise_for_status()  # Raise if HTTP error
            data = resp.json()
            print(f"{indent}Successfully fetched data for {cat_id}")
        except Exception as e:
            print(f"{indent}Error fetching category {cat_id}: {e}")
            return []

        namess = []
        parent_category = data.get("parentCategoryTree", [])
        for pp in parent_category:
            nam = pp.get("name", "")
            if "shop" not in nam.lower():
                namess.append(nam)

        categs = data.get("categories", [])
        has_subcats = bool(categs)

        if has_subcats:
            sub_ids = [ca.get("id") for ca in categs if ca.get("id")]
            leaves = []
            for sub_id in sub_ids:
                leaves.extend(get_leaf_categories(sub_id, headers, params, depth + 1))
            print(f"{indent}Collected {len(leaves)} leaves from subcategories of {cat_id}")
            return leaves
        else:
            print(f"{indent}This is a leaf category: {cat_id}")
            try:
                if len(namess) == 1:
                    category = namess[0]
                elif len(namess) == 2:
                    category = f"{namess[0]},{namess[0]} {namess[1]}"
                elif len(namess) == 3:
                    category = f"{namess[0]},{namess[0]} {namess[1]},{namess[0]} {namess[1]} {namess[2]}"
                elif len(namess) == 4:
                    category = f"{namess[0]},{namess[0]} {namess[1]},{namess[0]} {namess[1]} {namess[2]},{namess[0]} {namess[1]} {namess[2]} {namess[3]}"
                else:
                    category = ""
            except Exception as e:
                print(f"{indent}Error building category for {cat_id}: {e}")
                category = ""
            
            current_id = data.get("id")
            if current_id:
                leaf_entry = {"id": current_id, "category": category}
                print(f"{indent}Adding leaf entry: {leaf_entry}")
                return [leaf_entry]
            else:
                print(f"{indent}No ID found for leaf category {cat_id}")
                return []

    def categories_ids():
        print("Starting category fetch process...")

        params = {
            'locale': 'en-US',
            'siteId': 'CanadaGooseUS',
            'c_showInMenu': 'true',
        }

        main_data = get_leaf_categories(cat_id, headers, params)
        print(f"Process complete. Total leaf categories found: {len(main_data)}")
        return main_data
    
    data = categories_ids()
    return data


def get_product_ids(category_id):
    params_base = {
        'siteId': 'CanadaGooseUS',
        'refine': f'cgid={category_id}',
        'currency': 'USD',
        'locale': 'en-US',
        'expand': 'availability,images,prices,represented_products,variations,promotions,custom_properties,page_meta_tags',
        'allVariationProperties': 'true',
    }

    response1 = requests.get(
        'https://www.canadagoose.com/mobify/proxy/api/search/shopper-search/v1/organizations/f_ecom_aata_prd/product-search',
        params=params_base,
        headers=headers,
    )
    if response1.status_code != 200:
        print(f"Initial fetch failed: {response1.status_code}")
        return set()
    
    data1 = response1.json()
    total = data1.get("total", 0)
    print(f"Total products in category: {total}")

    if total == 0:
        return set()

    limit = 50  # Increase for efficiency; adjust if API caps it
    number_of_requests = (total + limit - 1) // limit  # Ceiling division for full coverage
    print(f"Will make {number_of_requests} requests (limit={limit})")

    product_ids = set()
    for i in range(number_of_requests):
        params = params_base.copy()
        params['offset'] = i * limit
        params['limit'] = limit

        response = requests.get(
            'https://www.canadagoose.com/mobify/proxy/api/search/shopper-search/v1/organizations/f_ecom_aata_prd/product-search',
            params=params,
            headers=headers,
        )
        if response.status_code == 200:
            data = response.json()
            hits = data.get("hits", [])
            new_ids = {hit.get("productId") for hit in hits if hit.get("productId")}

            product_ids.update(new_ids)
            print(f"Page {i}: Added {len(product_ids)} new IDs (total unique: {len(product_ids)})")
        else:
            print(f"Page {i} failed: {response.status_code}")

    return product_ids


def get_clean_json_for_products(product_id):
    params = {
        'currency': 'USD',
        'expand': 'availability,bundled_products,links,promotions,options,prices,variations,set_products,recommendations,page_meta_tags',
        'locale': 'en-US',
        'allImages': 'true',
        'siteId': 'CanadaGooseUS',
        'c_productlinks': 'true',
    }

    response = requests.get(
        f'https://www.canadagoose.com/mobify/proxy/api/product/shopper-products/v1/organizations/f_ecom_aata_prd/products/{product_id}',
        params=params,
        headers=headers
    )
    js_str = response.text.strip()
    return js_str


def get_parser(jso, category_name):
    try:
        data = json.loads(jso)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        return []

    def safe_get(d, key, default=""):
        return d.get(key, default) if isinstance(d, dict) else default

    sku = safe_get(data, "id")
    name = safe_get(data, "name")
    price = safe_get(data, "price", 0.0)
    brand = safe_get(data, "brand")
    
    try:
        desc1 = data.get("c_fsProductDescriptionShort", "").strip()
        desc2 = data.get("shortDescription", "").strip()
        desc3 = data.get("longDescription", "").strip()  # already may contain <ul>/<li> tags
        fill = data.get("c_fill", "").strip()
        fur = data.get("c_furDisplayValue", "").strip()
        composition = data.get("c_fabricCompositionDisplayValue", "").strip()
        care = data.get("c_care", "").strip()
        size_and_fit = data.get("c_customBulletPoints", "").strip()

        html_parts = []

        if desc1:
            html_parts.append(f'<p>{desc1}</p>')

        if desc2 or desc3:
            details_html = ""
            if desc2:
                details_html += f"<p>{desc2}</p>"
            if desc3:
                details_html += f"{desc3}"
            html_parts.append(f"<h3>Details</h3>{details_html}")

        if fur:
            html_parts.append(f"<h3>Fur</h3><p>{fur}</p>")

        if fill:
            html_parts.append(f"<h3>Fill</h3><p>{fill}</p>")
        if composition:
            html_parts.append(f"<h3>Composition</h3><p>{composition}</p>")

        if care:
            html_parts.append(f"<h3>Care</h3><p>{care}</p>")

        if size_and_fit:
            html_parts.append(f"<h3>Size and Fit</h3><p>{size_and_fit}</p>")

        desc = "\n".join(html_parts).strip()

    except Exception as e:
        desc = ""


    images = []
    image_suffix = "o.png"
    try:
        image_cache_str = safe_get(data, "c_cloudinaryImageObjectCache", "{}")
        image_cache = json.loads(image_cache_str) if isinstance(image_cache_str, str) else image_cache_str
        cloud_name = "canada-goose"
        base_url = f"https://res.cloudinary.com/{cloud_name}/image/upload/"
        transform = "q_auto,f_auto,c_fill/"
        
        for key, info in image_cache.items():
            version = safe_get(info, "version", "")
            public_id = safe_get(info, "public_id", "")
            fmt = safe_get(info, "format", "jpg")
            if version and public_id:
                url = f"{base_url}{transform}v{version}/{public_id}.{fmt}"
                images.append(url)
        
        if images:
            first_url = images[0]
            public_id_match = re.search(r'/product-image/([^.]+)\.', first_url)
            if public_id_match:
                full_public_id = public_id_match.group(1)
                # Last part after last '_': e.g., "1044M_9146_fsph" -> "fsph"
                image_suffix = full_public_id.split('_')[-1]
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"Image processing error: {e}")

    is_upload = 0

    sizes = []
    colors = set()
    variant_map = {}
    color_to_image = {}
    color_to_display = {}
    try:
        variation_attrs = safe_get(data, "c_variationAttributes", [])
        for attr in variation_attrs:
            if safe_get(attr, "name", "").lower() == "color":
                for value in safe_get(attr, "values", []):
                    color_id = safe_get(value, "id", "")
                    color_name = safe_get(value, "displayValue", color_id)
                    cloudinary_swatch = safe_get(value, "cloudinarySwatchImage", {})
                    version_number = safe_get(cloudinary_swatch, "version", "")
                    file_name = safe_get(cloudinary_swatch, "filename", "")
                    # FIXED: Use dynamic suffix instead of hardcoded "_o.png"
                    color_image = f"https://res.cloudinary.com/canada-goose/image/upload/q_auto,f_auto,c_fill/v{version_number}/product-image/{sku}_{file_name}_{image_suffix}" if version_number and file_name else ""
                    if color_id:
                        color_to_display[color_id] = color_name
                        color_to_image[color_id] = color_image
                        colors.add((color_name, color_image))
                break

        variants = safe_get(data, "variants", [])
        for variant in variants:
            if not safe_get(variant, "orderable", False):
                continue
            variation_values = safe_get(variant, "variationValues", {})
            size = safe_get(variation_values, "Size", "")

            color_id = safe_get(variation_values, "Color", "")
            variant_id = safe_get(variant, "productId", "")
            if size and color_id:
                variant_map[variant_id] = {"size": size, "color": color_id}
                
                if size not in [s["size"] for s in sizes]:
                    sizes.append({"size": size, "variant_id": variant_id})

        def extract_number(s):
            num_str = re.sub(r"[^\d.]", "", s["size"])
            try:
                return float(num_str)
            except ValueError:
                return None

        numbers = [extract_number(sz) for sz in sizes]
        if all(n is not None for n in numbers):
            sizes.sort(key=lambda x: (extract_number(x), x["size"]))
        else:
            size_order = {"XXS": 0, "XS": 1, "S": 2, "M": 3, "L": 4, "XL": 5, "XXL": 6, "XXXL": 7}
            sizes.sort(key=lambda x: (size_order.get(x["size"].upper(), 100), x["size"]))

        colors = [{"color": name, "url": image} for name, image in sorted(colors, key=lambda x: x[0])]
    except Exception as e:
        print(f"Variation processing error: {e}")

    num_sizes = len(sizes)
    num_colors = len(colors)
    is_variable = num_sizes > 1 or num_colors > 1
    typ = "variable" if is_variable else "simple"

    size_list = [s["size"] for s in sizes]
    all_sizes_str = ",".join(size_list) if size_list else ""
    single_size_str = size_list[0] if size_list else ""

    formatted_colors = [f"{col['color']}" if col["color"] else "" for col in colors]
    all_colors_str = ",".join(formatted_colors) if formatted_colors else ""
    single_color_str = formatted_colors[0] if formatted_colors else ""

    attr1_name = ""
    attr1_values = ""
    attr2_name = ""
    attr2_values = ""
    if typ == "variable":
        if num_colors > 0:
            attr1_name = "color"
            attr1_values = all_colors_str if num_colors > 1 else single_color_str
        if num_sizes > 0:
            attr2_name = "size"
            attr2_values = all_sizes_str if num_sizes > 1 else single_size_str
    else:
        if num_colors > 0:
            attr1_name = "color"
            attr1_values = single_color_str
        if num_sizes > 0:
            attr2_name = "size"
            attr2_values = single_size_str

    images_str = ",".join(images) if images else ""
    regular_price = price

    row = {
        "Type": typ,
        "SKU": sku,
        "Name": name,
        "Description": desc,
        "Sale price": regular_price,
        "Regular price": regular_price,
        "Categories": category_name,        
        "Tags": "",
        "Images": images_str,
        "Parent": "",
        "Attribute 1 name": attr1_name,
        "Attribute 1 value(s)": attr1_values,
        "Attribute 2 name": attr2_name,
        "Attribute 2 value(s)": attr2_values,       
        "brand": brand,
        "Stock": 1000,
        "is_upload": is_upload,
    }
    rows = [row]

    # Generate variation rows based on actual variants
    if typ == "variable":
        for variant_id, vdata in variant_map.items():
            size = vdata["size"]
            color_id = vdata["color"]
            color_display = color_to_display.get(color_id, color_id)
            if size and color_display:
                var_row = row.copy()
                var_row["Type"] = "variation"
                var_row["Parent"] = sku
                var_images = color_to_image.get(color_id, images_str)
                var_row["Images"] = var_images if isinstance(var_images, str) else ",".join(var_images)
                var_row["Attribute 1 value(s)"] = color_display
                var_row["Attribute 2 value(s)"] = size
                var_sku = f"{sku}-{color_id.upper()}-{size.upper()}"
                var_row["SKU"] = var_sku
                var_row["Stock"] = 1000
                rows.append(var_row)

    return rows

lst = ["shop-mens", "shop-womens", "shop-new-arrivals-unisex", "shop-kids", "shop-shoes"]

def process_product(product_id, category_name):
    """Helper: Fetch JSON and parse for a single product (runs in thread)."""
    js_string = get_clean_json_for_products(product_id=product_id)
    data = get_parser(jso=js_string, category_name=category_name)  # Assuming get_parser is defined elsewhere
    return data


def main():
    top_categories = ["shop-mens", "shop-womens", "shop-new-arrivals-unisex", "shop-kids", "shop-shoes"]
    all_data = []
    
    start_time = time.time()
    
    for top_cat in top_categories:
        print(f"\n=== Processing top category: {top_cat} ===")
        leaf_categories = get_id_and_category(top_cat)
        for leaf in leaf_categories:
            id_ = leaf.get("id")
            category_name = leaf.get("category")
            print(f"Working on category {category_name} and id {id_}")
            product_ids = get_product_ids(category_id=id_)

            with ThreadPoolExecutor(max_workers=50) as executor:  # Increased for speed
                futures = [executor.submit(process_product, pid, category_name) for pid in product_ids]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        data = future.result()
                        all_data.extend(data)
                    except Exception as e:
                        print(f"Error processing product: {e}")

    elapsed = time.time() - start_time
    print(f"\nTotal processing time: {elapsed:.2f} seconds")
    
    return all_data


rows = main()
df = pd.DataFrame(rows)
df.to_csv("all_data.csv", index=False)
print("Saved clean CSV. Sample head:\n", df.head())

