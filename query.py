import boto3, time
import json
import trp.trp2 as t2
import trp.trp2_analyzeid as t2id

S3_BUCKET = "Name_of_S3Bucket"
S3_KEY = "PDF form to analyse"

aws_client_textract = boto3.client("textract")
client = boto3.client('s3')

try:

    ####
    # trigger job
    ####

    job = aws_client_textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': S3_BUCKET,
                'Name': S3_KEY
            }
        },
        FeatureTypes=[
            'FORMS',
            'QUERIES'
        ],
        
        QueriesConfig={
            "Queries": [
                {
                    "Text": "Query 1",
                    "Alias":"Alias 1",
                    "Pages":["Page #"]
                },
                {
                    "Text": "Query 2",
                    "Alias":"Alias 2",
                    "Pages":["Page # eg 2"]
                },
                {
                    "Text":"Query 3",
                    "Alias":"Alias3",
                    "Pages":["Page # eg 3"]
                },
            ]
        },
        
        OutputConfig={
            'S3Bucket': S3_BUCKET,
            'S3Prefix': f"output"
        }
    )

    ####
    # wait for document to finish
    ####

    document = False
    while(True):
        time.sleep(2)

        document = aws_client_textract.get_document_analysis(
            JobId = job['JobId']
        )   
        if(document['JobStatus'] in ["SUCCEEDED"]):
            break

    ####
    # print values of interest
    ####

#    for b in document['Blocks']:
#        print(b)
#        if(b['BlockType'] in ["WORD"]):
#            if(b['TextType']=='HANDWRITING'):
#              print(json.dumps(b,indent = 4))
#                print(b['Page'])
#                print(b['Text'])
#                print(b['TextType'])
#                print(b['Confidence'])
        
#    for b in document['Blocks']:
#        if(b['BlockType'] in ["QUERY"]):
#           if(b['Type'] == 'ANSWER'):
#                print(json.dumps(b,indent = 4))
#                print(b)
     
    questions = {
        # "id": {"text": "", answers": []}
    }
    
    # iterate blocks and create questions (and links to answers)
    for b in document['Blocks']:
        if(b['BlockType'] == "QUERY"):
            tmp = {
                'text': b['Query']['Text'],
                'answers_ids': [],
                'answers_processed': {}
            }
            if ('Relationships' in b):
                for r in b['Relationships']:
                    if(r['Type'] == "ANSWER"):
                        tmp['answers_ids'] = tmp['answers_ids'] + r['Ids']
    
            questions[b['Id']] = tmp
            
    # iterate blocks and answer questions
    for b in document['Blocks']:
        if(b['BlockType'] == "QUERY_RESULT"):
            for q in questions:
                if(b['Id'] in questions[q]['answers_ids']):
                    questions[q]['answers_processed'][b['Id']] = {
                        'confidence': b['Confidence'],
                        'page': b['Page'],
                        'text': b['Text']
                    }
    
    
    # return query, answer and confidence
    file_content=""
    FILE_NAME="Nmae_of_output_object"
    for q in questions:
        print(f"q: {questions[q]['text']}")
        for a in questions[q]['answers_processed']:
            response = (f"q: {questions[q]['text']} \n a:{questions[q]['answers_processed'][a]['text']} ({questions[q]['answers_processed'][a]['confidence']}%)\n")  
            file_content=file_content + '\n' + response
            print(file_content)
            client.put_object(
                Body=file_content, 
                Bucket=S3_BUCKET, 
                Key=FILE_NAME
            )

        
except Exception as e:
    print(e)
