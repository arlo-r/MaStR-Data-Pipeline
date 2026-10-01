import glob
import os
import re
from lxml import etree
import polars as pl

CHUNK_SIZE = 100_000  # Number of rows per temporary chunk file


def parse_xml_file_to_parquet(file_path: str, temp_output_dir: str) -> list[str]:
    """Parse a large XML file in streaming chunks, forcing all columns to Utf8 (String)."""
    base_filename = os.path.basename(file_path).replace('.xml', '')
    created_chunks = []
    rows = []
    chunk_index = 0

    try:
        context = etree.iterparse(
            file_path, events=('end',), remove_blank_text=True
        )

        for event, elem in context:
            if len(elem) > 0 and elem.tag != 'Gesamtdatenabzug':
                row = {
                    child.tag: str(child.text) if child.text is not None else None
                    for child in elem
                    if child.tag is not None
                }
                if row:
                    rows.append(row)

                # Memory cleanup
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

                # Flush chunk to disk when size limit is reached
                if len(rows) >= CHUNK_SIZE:
                    chunk_file = os.path.join(
                        temp_output_dir, f'{base_filename}_part{chunk_index}.parquet'
                    )

                    df = pl.DataFrame(rows, infer_schema_length=None)
                    df = df.select([pl.col(col).cast(pl.Utf8) for col in df.columns])

                    df.write_parquet(chunk_file, compression='zstd')
                    created_chunks.append(chunk_file)
                    rows.clear()
                    chunk_index += 1

        # Save remaining rows for the last chunk
        if rows:
            chunk_file = os.path.join(
                temp_output_dir, f'{base_filename}_part{chunk_index}.parquet'
            )
            df = pl.DataFrame(rows, infer_schema_length=None)
            df = df.select([pl.col(col).cast(pl.Utf8) for col in df.columns])
            df.write_parquet(chunk_file, compression='zstd')
            created_chunks.append(chunk_file)
            rows.clear()

    except Exception as e:
        print(f"Error parsing {file_path}: {e}")

    return created_chunks


def get_file_groups(input_dir: str) -> dict[str, list[str]]:
    """Scan directory and group XML files automatically by their base prefix."""
    xml_files = sorted(glob.glob(os.path.join(input_dir, '*.xml')))
    groups = {}

    for file_path in xml_files:
        filename = os.path.basename(file_path)
        base_name = re.sub(r'_\d+\.xml$', '', filename)
        base_name = re.sub(r'\.xml$', '', base_name)

        if base_name not in groups:
            groups[base_name] = []
        groups[base_name].append(file_path)

    return groups


def process_group(group_name: str, file_list: list[str], output_dir: str):
    """Process a group of XML files safely with schema alignment using diagonal concat."""
    final_output = os.path.join(output_dir, f'{group_name}.parquet')
    temp_dir = os.path.join(output_dir, f'_temp_{group_name}')

    if os.path.exists(final_output):
        print(f"Skipping '{group_name}' (Already exists).")
        return

    print(f"[{group_name}] Processing {len(file_list)} files...")
    os.makedirs(temp_dir, exist_ok=True)

    all_chunk_files = []
    for xml_file in file_list:
        chunks = parse_xml_file_to_parquet(xml_file, temp_dir)
        all_chunk_files.extend(chunks)

    if not all_chunk_files:
        print(f"[{group_name}] Warning: No valid data found.")
        return

    print(f"[{group_name}] Merging {len(all_chunk_files)} chunk files into final Parquet...")

    # Read all chunk files into DataFrames and concatenate using diagonal method
    dfs = [pl.read_parquet(f) for f in all_chunk_files]
    combined_df = pl.concat(dfs, how="diagonal")
    combined_df.write_parquet(final_output, compression='zstd')

    # Clean up temporary chunk files and folder
    for f in all_chunk_files:
        try:
            os.remove(f)
        except OSError:
            pass
    try:
        os.rmdir(temp_dir)
    except OSError:
        pass

    print(f"[{group_name}] Successfully created: {final_output}\n")


def convert_all_mastr_data(input_dir: str, output_dir: str):
    """Automatically discover all XML groups and convert them to Parquet."""
    os.makedirs(output_dir, exist_ok=True)

    groups = get_file_groups(input_dir)
    print(f"Discovered {len(groups)} distinct datasets in: {input_dir}\n")

    for group_name, file_list in groups.items():
        process_group(group_name, file_list, output_dir)

