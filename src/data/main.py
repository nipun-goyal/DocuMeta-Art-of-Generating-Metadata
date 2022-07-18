from concurrent.futures import process
from os import listdir, remove
from os.path import isfile, join
from tika import parser
import pickle
from multiprocessing import Pool
import time
from pathlib import Path
import glob

ROOT_PATH = Path(__file__).resolve().parents[2]

pdfs_path = str(ROOT_PATH / "data" / "raw" / "pdfs")
text_files_dir = str(ROOT_PATH / "data" / "processed" / "text_files")

#constants.save_dir + "/" + "all_tables-final.csv",

def get_pdf():
    pdf_files = [f for f in listdir(ROOT_PATH / "data" / "raw" / "pdfs") if isfile(join(ROOT_PATH / "data" / "raw" / "pdfs", f))]
    return pdf_files


def extract_text(pdf):
    bad_files = []
    name = pdf[:-4]
    results = parser.from_file(pdfs_path + "/" + pdf)
    if results['content'] is None:
        bad_files.append(pdf)
    else:
        file_name = text_files_dir + "/" + name + ".txt"
        text_file = open(file_name, "w", encoding = 'utf-8')  
        text_file.write(results['content'])
        text_file.close()
    return bad_files

def process_handler():
    start_time = time.time()

    pdfs = get_pdf()
    with Pool() as pool:
        results = pool.map(extract_text, pdfs, chunksize=1)
    with open('bad_files1.txt', 'w') as f:
        for result in results:
            if len(result) != 0:
                f.write(result[0] + "\n")
    
    duration = round(time.time() - start_time)
    print(f"Text extracted and populated in {duration} seconds ({round(duration / 60, 2)} min or {round(duration / 3600, 2)} hours)")

if __name__ == "__main__":
    process_handler()
