import openai
import json
import time

openai.api_key = "your-api-key"  # or use env var

MODEL = "gpt-4-1106-preview"  # GPT-4.1
INPUT_FILE = "prompts.txt"    # or use a JSONL if already formatted

# Step 1: Load and format prompts
def create_jsonl_from_prompts(file_path, output_jsonl):
    with open(file_path, "r") as f:
        prompts = [line.strip() for line in f.readlines() if line.strip()]

    requests = []
    for prompt in prompts:
        requests.append({
            "custom_id": f"req-{len(requests)}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": MODEL,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 100,
                "temperature": 0
            }
        })

    with open(output_jsonl, "w") as f:
        for req in requests:
            f.write(json.dumps(req) + "\n")

    print(f"Saved {len(requests)} requests to {output_jsonl}")

# Step 2: Upload file to OpenAI
def upload_batch_file(file_path):
    with open(file_path, "rb") as f:
        uploaded_file = openai.files.create(file=f, purpose="batch")
    print("Uploaded file ID:", uploaded_file.id)
    return uploaded_file.id

# Step 3: Create batch
def create_batch(file_id):
    batch = openai.batches.create(
        input_file_id=file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    print("Batch ID:", batch.id)
    print("Status:", batch.status)
    return batch.id

# Step 4 (Optional): Monitor batch status
def check_batch_status(batch_id):
    while True:
        batch = openai.batches.retrieve(batch_id)
        print(f"Status: {batch.status}")
        if batch.status in ["completed", "failed", "expired"]:
            break
        time.sleep(30)  # wait before checking again

# Run the steps
create_jsonl_from_prompts(INPUT_FILE, "batch_requests.jsonl")
file_id = upload_batch_file("batch_requests.jsonl")
batch_id = create_batch(file_id)
# check_batch_status(batch_id)  # uncomment to poll status
