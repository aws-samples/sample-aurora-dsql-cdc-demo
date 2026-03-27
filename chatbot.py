#!/usr/bin/env python3
"""
Semantic search chatbot for DSQL CDC data using S3 Vectors
"""
import warnings
warnings.filterwarnings('ignore')

import json
import boto3
import sys

# Initialize clients
bedrock = boto3.client('bedrock-runtime', region_name='us-east-2')
s3vectors = boto3.client('s3vectors', region_name='us-east-2')
s3 = boto3.client('s3', region_name='us-east-2')

VECTOR_BUCKET = 'dsql-cdc-vectors'
INDEX_NAME = 'cdc-events'
DATA_BUCKET = 'dsql-cdc-processed-771062417019'

def generate_embedding(text):
    """Generate embedding for query text"""
    response = bedrock.invoke_model(
        modelId='amazon.titan-embed-text-v2:0',
        body=json.dumps({"inputText": text})
    )
    result = json.loads(response['body'].read())
    return result['embedding']

def search_vectors(query_text, top_k=10):
    """Search for similar vectors"""
    print(f"🔍 Searching for: {query_text}\n")
    
    # Generate query embedding
    query_vector = generate_embedding(query_text)
    
    # Query S3 Vectors
    response = s3vectors.query_vectors(
        vectorBucketName=VECTOR_BUCKET,
        indexName=INDEX_NAME,
        queryVector={'float32': query_vector},
        topK=top_k,
        returnMetadata=True,
        returnDistance=True
    )
    
    return response['vectors']

def get_cdc_data(vector_key):
    """Retrieve full CDC event data from S3"""
    try:
        response = s3.get_object(
            Bucket=DATA_BUCKET,
            Key=f"data/{vector_key}.json"
        )
        return json.loads(response['Body'].read())
    except:
        return None

def answer_question(question):
    """Answer question using semantic search + LLM"""
    
    # Increase results for "list all" or "show all" queries
    top_k = 50 if any(word in question.lower() for word in ['all', 'list', 'show']) else 10
    
    # Step 1: Search for relevant vectors
    results = search_vectors(question, top_k=top_k)
    
    if not results:
        print("❌ No results found")
        return
    
    print(f"📊 Found {len(results)} relevant records\n")
    
    # Step 2: Retrieve full CDC data (parallel for speed)
    import concurrent.futures
    
    def fetch_data(result):
        vector_key = result['key']
        distance = result.get('distance', 0)
        metadata = result.get('metadata', {})
        cdc_data = get_cdc_data(vector_key)
        return (vector_key, distance, metadata, cdc_data)
    
    context_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_data, result) for result in results]
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            vector_key, distance, metadata, cdc_data = future.result()
            if cdc_data:
                context_data.append(cdc_data['cdc_event'])
                if i <= 10:  # Only show first 10 in output
                    after_data = cdc_data['cdc_event'].get('after', {})
                    if after_data:
                        preview = {k: v for k, v in list(after_data.items())[:3]}
                        print(f"{i}. Table: {metadata.get('table', 'unknown')} | "
                              f"Action: {metadata.get('action', 'unknown')} | "
                              f"Distance: {distance:.3f}")
                        print(f"   Preview: {preview}")
    
    if len(results) > 10:
        print(f"... and {len(results) - 10} more records")
    
    print(f"\n💡 Generating answer...\n")
    
    # Step 3: Use LLM to answer question with context
    # Change MODEL_ID to switch between models:
    # - 'us.amazon.nova-lite-v1:0' (fast, cheap)
    # - 'us.anthropic.claude-sonnet-4-20250514-v1:0' (better quality)
    MODEL_ID = 'us.anthropic.claude-sonnet-4-20250514-v1:0'
    
    prompt = f"""Based on the following database records, answer this question: {question}

Database Records:
{json.dumps(context_data, indent=2)}

Provide a clear, concise answer based only on the data provided. 
Explain which records you used to answer the question."""
    
    response = bedrock.converse(
        modelId=MODEL_ID,
        messages=[{
            'role': 'user',
            'content': [{'text': prompt}]
        }]
    )
    
    answer = response['output']['message']['content'][0]['text']
    print(f"✅ Answer:\n{answer}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./chatbot.py 'your question here'")
        print("\nExample questions:")
        print("  ./chatbot.py 'give me all orders for Sarah Johnson'")
        print("  ./chatbot.py 'what products did customer 2 buy?'")
        print("  ./chatbot.py 'show me all 5-star reviews'")
        sys.exit(1)
    
    question = ' '.join(sys.argv[1:])
    answer_question(question)
