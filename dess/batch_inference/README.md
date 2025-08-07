# Batch Inference Pipeline

## Overview

The Batch Inference Pipeline is a component of the DESS project that provides a structured way to process large batches of faculty data through LLM (Large Language Model) inference to extract department information.

This component follows the Factory design pattern to support multiple model providers (currently Gemini and OpenAI) while maintaining a consistent interface for batch processing.

## Architecture

The component follows a standard abstract factory pattern:

```
BatchInferencePipeline (abstract)
├── GeminiBatchInferencePipeline
└── OpenAIBatchInferencePipeline

BatchInferencePipelineFactory
```

Each pipeline implementation exposes five key methods:
1. `prepare_batch_file`: Convert DataFrame to provider-specific batch format (typically JSONL files)
2. `upload_batch_file`: Upload batch file to appropriate storage (GCS for Vertex)
3. `create_batch_job`: Initialize batch processing job
4. `get_batch_status`: Check current status of batch job
5. `retrieve_and_merge_results`: Collect results and update DataFrame

## Key Features

- **Unified Interface**: Common pipeline stages across different model providers
- **Batch Processing**: Efficiently handle large datasets by processing in batches
- **Provider Agnostic**: Abstract away provider-specific implementation details
- **Error Handling**: Graceful handling of failures during batch processing
- **Progress Tracking**: Monitor batch job status during execution



## Usage

```python
from dess.batch_inference.factory import BatchInferencePipelineFactory
import pandas as pd

# Initialize pipeline based on provider
pipeline = BatchInferencePipelineFactory.get_pipeline("gemini-2.0-flash-001")

# Prepare batch file
batch_info = pipeline.prepare_batch_file(df, "batch.jsonl")

# Upload to storage (GCS for Gemini)
source_uri = pipeline.upload_batch_file("batch.jsonl", "dess-llm-jobs")

# Create and run batch job
job = pipeline.create_batch_job(source_uri, "gs://dess-llm-jobs")

# Check status and wait for completion
job = pipeline.wait_for_completion(job)

# Retrieve results and merge back to DataFrame
updated_df = pipeline.retrieve_and_merge_results(job, df, batch_info)
```

For convenience, you can use the `extract_departments_batch.py` script which wraps all these steps:

```bash
python -m dess.extract_departments_batch --input_file=data.parquet --provider=gemini-2.0-flash-001
```

## Provider-Specific Details

### Gemini

The Gemini implementation uses Google Cloud Storage for batch file storage and Vertex AI for batch processing. It requires:
- Google API key set in the environment (`GOOGLE_API_KEY`)
- A GCS bucket for batch file storage
- Gemini model name (e.g., "gemini-2.0-flash-001")

### OpenAI

The OpenAI implementation doesn't use cloud storage but rather simulates batch processing by chunking requests. It requires:
- OpenAI API key set in the environment (`OPENAI_API_KEY`)
- Model name (e.g., "gpt-3.5-turbo")

## Extending

To add support for a new model provider:

1. Create a new class that inherits from `BatchInferencePipeline`
2. Implement all required methods with provider-specific logic
3. Update the `BatchInferencePipelineFactory` to return your new class

## Integration with DESS

This component is used in the department extraction pipeline to provide an alternative to regex-based department extraction, using LLMs to identify departments from faculty information.

The results are stored in the `department_llm` column of the output DataFrame, which can be compared with or used alongside the pattern-based extraction methods. 