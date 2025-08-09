from prefect import flow, task
import pandas as pd
import random
import os
import requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from datetime import datetime




# Make sure pdf-output folder exists
if not os.path.exists("pdf-output"):
    os.makedirs("pdf-output")

# Task 1: Read the CSV file and pick 3 random entries
@task
def pick_random_items():
    # Randomly choose dataset
    dataset = random.choice(["animals", "movies"])
    csv_file = f"random-{dataset}.csv"
    
    # Read CSV
    df = pd.read_csv(csv_file)
    if df.empty:
     raise ValueError(f"The file {csv_file} is empty!")

    
    # Pick 3 random entries
    selected = df.sample(3).reset_index(drop=True)

    print(f"Picked 3 random items from {dataset}:")
    print(selected)

    return dataset, selected
# Task 2: Download images from URLs
@task
def download_images(selected_items):
    image_files=[]
    for index, row in selected_items.iterrows():
        name = row["name"]
        url = row["image_url"]

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise error if status not 200

            # Check if it's really an image
            if "image" not in response.headers.get("Content-Type", ""):
                print(f"Skipping {name} - URL is not an image: {url}")
                continue

            file_path = f"temp_image_{index}.jpg"
            with open(file_path, "wb") as f:
                f.write(response.content)

            image_files.append((name, file_path))
        except Exception as e:
            print(f"Error downloading {name} from {url}: {e}")
            continue

    return image_files
    
# Task3: Generate the PDF
def create_pdf(dataset,image_files):
    timestamp=datetime.now().isoformat(timespec='minutes').replace(":", "-")
    pdf_filename = f"pdf-output/{dataset}-{timestamp}.pdf"
    c = canvas.Canvas(pdf_filename, pagesize=A4)
    width, height = A4
    for name, img_path in image_files:
        # Page with name
        c.setFont("Helvetica-Bold", 24)
        c.drawCentredString(width / 2, height / 2, name)
        c.showPage()

        # Page with image
        c.drawImage(img_path, 50, 100, width - 100, height - 200)
        c.showPage()
    c.save()
    print(f"PDF saved as {pdf_filename}")
    # Delete temp images
    for _, img_path in image_files:
        os.remove(img_path)
           
            
        
    



# Flow: Prefect main flow that calls the task
@flow
def pdf_generator_flow():
    dataset, selected_items = pick_random_items()
    image_files = download_images(selected_items)
    create_pdf(dataset, image_files)

# Run the flow

    pdf_generator_flow()
   
# --- 5. Scheduling (BOTTOM OF FILE) ---
if __name__ == "__main__":
    pdf_generator_flow.serve(
        name="csv-to-pdf-generator-deployment",
        cron="* * * * *"  # every 1 minute
    )
    
