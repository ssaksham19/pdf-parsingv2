import oci
import os
import openpyxl
import csv
import pandas as pd
from fdk import response
import io
import json
import logging
import PyPDF2
logging.basicConfig(level=logging.INFO)

def handler(ctx, data: io.BytesIO=None):
    object_name = source_bucket = target_bucket = namespace = ""
    try:
        body = json.loads(data.getvalue())
        object_name = body["file_name"]
        source_bucket = body["source_bucket"]
        namespace = body["namespace"]
        result = parsed_result(namespace, source_bucket, target_bucket, object_name)
    except Exception as ex:
        print(ex, flush=True)
        raise
    return response.Response(
        ctx,
        response_data=result,
        headers={"Content-Type": "application/json"}
    )

def parsed_result(namespace, source_bucket, object_name):
    # initialise client
    signer = oci.auth.signers.get_resource_principals_signer()
    object_storage = oci.object_storage.ObjectStorageClient({}, signer=signer)

    try:
        #set variables
        report_path = '/tmp/source_files'   #local temp storage to save the file before upload to object storage

        # extract filename without extension
        FileName_noExt = os.path.splitext(object_name)[0]

        logging.info('FUNCTION:GETFILES BEGIN...')

        # create local directory
        if not os.path.exists(report_path):
            os.mkdir(report_path)

        #set file paths and names
        source_path_file = report_path + '/' + object_name
        #dest_path_file = report_path + '/' + FileName_noExt + ".csv"
        #dest_file_name = FileName_noExt + ".csv"

        logging.info('source_path_file=' + source_path_file)
        #logging.info('dest_path_file=' + dest_path_file)
        #logging.info('dest_file_name=' + dest_file_name)

        #get object storage file and save locally
        with open(source_path_file, 'wb') as f:
            object_details = object_storage.get_object(namespace,source_bucket,object_name)
            for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)
                logging.info('finished downloading ' + object_name)


        #depending on file type handle differently
        #xls file
        if object_name[-1] == 'f':
            #read_file = pd.read_excel(source_path_file)
            #read_file.to_csv(dest_path_file, index = None, header=True)
            pdf = open(source_path_file, "rb")
            reader = PyPDF2.PdfFileReader(pdf)
            page = reader.getPage(0)
            parsed=page.extract_text()
            #print(page.extractText())       
        #upload final csv file to bucket
        #uploadmgr = oci.object_storage.UploadManager(object_storage)
        #sr = uploadmgr.upload_file(namespace,target_bucket, dest_file_name ,dest_path_file, content_type='text/csv')


        if page.__contains__("resume"):
            resp = json.dumps({"fileconvertion":"SUCCESS", "resume_flag":"True"})
            logging.info('File contains the word resume')
        else:
            resp = json.dumps({"fileconvertion":"SUCCESS", "resume_flag":"False"})
        #OPTIONALLY: cleanup origin Excel file from source bucket
        cleanup_resp = object_storage.delete_object(namespace, source_bucket, object_name)
        logging.info(cleanup_resp)

    except Exception as e:
        inst = str(e)
        resp = {"fileconvert":inst}

    # data for function response..
    return resp