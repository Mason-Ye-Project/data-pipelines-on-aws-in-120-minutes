"""Generate the small CloudFormation template from local, readable Python sources."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def ref(name):
    return {"Ref": name}


def sub(text):
    return {"Fn::Sub": text}


def attr(name, field="Arn"):
    return {"Fn::GetAtt": [name, field]}


def policy(statements):
    return {"Version": "2012-10-17", "Statement": statements}


def allow(actions, resources):
    return {"Effect": "Allow", "Action": actions, "Resource": resources}


def trust(service):
    return policy([{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}])


def build():
    code = (HERE / "transform.py").read_text() + "\n" + (HERE / "handler.py").read_text().replace(
        "from transform import MAX_BYTES, check_batch_id, json_lines, transform\n", "")
    tags = [{"Key": "BookLab", "Value": "data-pipelines-120-book"}, {"Key": "LabRun", "Value": ref("LabName")}]
    bucket_arn = sub("arn:${AWS::Partition}:s3:::${LabBucket}")
    columns = [{"Name": name, "Type": typ} for name, typ in [
        ("batch_id", "string"), ("order_id", "string"), ("customer_id", "string"),
        ("order_date", "string"), ("country", "string"), ("amount_cents", "bigint")]]
    definition = {
        "Comment": "Validate a small immutable raw batch, then reconcile it with Athena.",
        "StartAt": "Transform", "TimeoutSeconds": 180,
        "States": {
            "Transform": {
                "Type": "Task", "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": "${FunctionArn}", "Payload.$": "$"},
                "OutputPath": "$.Payload", "TimeoutSeconds": 35,
                "Retry": [{"ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException",
                                           "Lambda.SdkClientException", "Lambda.TooManyRequestsException"],
                           "IntervalSeconds": 2, "MaxAttempts": 2, "BackoffRate": 2}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}], "Next": "Reconcile"},
            "Reconcile": {
                "Type": "Task", "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                "Parameters": {
                    "QueryString.$": "$.reconciliation_sql",
                    "QueryExecutionContext": {"Database": "${DatabaseName}"},
                    "WorkGroup": "${WorkgroupName}"},
                "ResultPath": "$.athena", "TimeoutSeconds": 120,
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}], "Next": "ReadResults"},
            "ReadResults": {
                "Type": "Task", "Resource": "arn:aws:states:::athena:getQueryResults",
                "Parameters": {"QueryExecutionId.$": "$.athena.QueryExecution.QueryExecutionId", "MaxResults": 2},
                "ResultSelector": {
                    "rows.$": "States.StringToJson($.ResultSet.Rows[1].Data[0].VarCharValue)",
                    "amount.$": "States.StringToJson($.ResultSet.Rows[1].Data[1].VarCharValue)"},
                "ResultPath": "$.reconciliation", "TimeoutSeconds": 15,
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}], "Next": "CheckTotals"},
            "CheckTotals": {
                "Type": "Choice", "Choices": [{"And": [
                    {"Variable": "$.reconciliation.rows", "NumericEqualsPath": "$.accepted_rows"},
                    {"Variable": "$.reconciliation.amount", "NumericEqualsPath": "$.accepted_amount_cents"}],
                    "Next": "Succeeded"}], "Default": "Mismatch"},
            "Succeeded": {"Type": "Succeed"},
            "Mismatch": {"Type": "Fail", "Error": "ReconciliationMismatch", "Cause": "Athena totals differ from the transformation receipt."},
            "Failed": {"Type": "Fail", "Error": "BatchFailed", "Cause": "Inspect the failed step; fix the cause before retrying."},
        },
    }
    resources = {
        "LabBucket": {"Type": "AWS::S3::Bucket", "Properties": {
            "BucketName": ref("LabName"), "Tags": tags,
            "PublicAccessBlockConfiguration": {"BlockPublicAcls": True, "IgnorePublicAcls": True,
                                                "BlockPublicPolicy": True, "RestrictPublicBuckets": True},
            "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
            "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
            "LifecycleConfiguration": {"Rules": [{"Id": "AbortIncomplete", "Status": "Enabled",
                                                   "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]},
        }},
        "BucketTLS": {"Type": "AWS::S3::BucketPolicy", "Properties": {"Bucket": ref("LabBucket"),
            "PolicyDocument": policy([{"Effect": "Deny", "Principal": "*", "Action": "s3:*",
                "Resource": [bucket_arn, sub("${LabBucket.Arn}/*")], "Condition": {"Bool": {"aws:SecureTransport": "false"}}}])}},
        "FunctionLog": {"Type": "AWS::Logs::LogGroup", "Properties": {
            "LogGroupName": sub("/aws/lambda/${LabName}"), "RetentionInDays": 1, "Tags": tags}},
        "FunctionRole": {"Type": "AWS::IAM::Role", "Properties": {
            "AssumeRolePolicyDocument": trust("lambda.amazonaws.com"), "Tags": tags,
            "Policies": [{"PolicyName": "TransformOwnBatch", "PolicyDocument": policy([
                allow(["s3:GetObject"], sub("${LabBucket.Arn}/raw/*")),
                allow(["s3:PutObject"], [sub("${LabBucket.Arn}/curated/*"), sub("${LabBucket.Arn}/quarantine/*"),
                                           sub("${LabBucket.Arn}/receipts/*")]),
                allow(["logs:CreateLogStream", "logs:PutLogEvents"], attr("FunctionLog")),
            ])}]}},
        "Transformer": {"Type": "AWS::Lambda::Function", "DependsOn": "FunctionLog", "Properties": {
            "FunctionName": ref("LabName"), "Runtime": "python3.12", "Handler": "index.lambda_handler",
            "Role": attr("FunctionRole"), "MemorySize": 256, "Timeout": 30,
            "Environment": {"Variables": {"LAB_BUCKET": ref("LabBucket")}}, "Code": {"ZipFile": code}, "Tags": tags}},
        "Database": {"Type": "AWS::Glue::Database", "Properties": {
            "CatalogId": ref("AWS::AccountId"), "DatabaseInput": {"Name": ref("DatabaseName"), "Description": "Synthetic orders book lab"}}},
        "OrdersTable": {"Type": "AWS::Glue::Table", "Properties": {
            "CatalogId": ref("AWS::AccountId"), "DatabaseName": ref("Database"),
            "TableInput": {"Name": "orders", "TableType": "EXTERNAL_TABLE", "Parameters": {"classification": "json", "EXTERNAL": "TRUE"},
                "StorageDescriptor": {"Columns": columns, "Location": sub("s3://${LabBucket}/curated/"),
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.apache.hive.hcatalog.data.JsonSerDe"}}}}},
        "Workgroup": {"Type": "AWS::Athena::WorkGroup", "Properties": {
            "Name": ref("LabName"), "State": "ENABLED", "RecursiveDeleteOption": True, "Tags": tags,
            "WorkGroupConfiguration": {"EnforceWorkGroupConfiguration": True, "BytesScannedCutoffPerQuery": 10_000_000,
                "PublishCloudWatchMetricsEnabled": False, "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
                "ResultConfiguration": {"OutputLocation": sub("s3://${LabBucket}/results/"),
                                        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}}}}},
        "WorkflowRole": {"Type": "AWS::IAM::Role", "Properties": {
            "AssumeRolePolicyDocument": policy([{"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole", "Condition": {"StringEquals": {"aws:SourceAccount": ref("AWS::AccountId")},
                "ArnLike": {"aws:SourceArn": sub("arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:stateMachine:${LabName}")}}}]),
            "Tags": tags, "Policies": [{"PolicyName": "RunOwnPipeline", "PolicyDocument": policy([
                allow(["lambda:InvokeFunction"], attr("Transformer")),
                allow(["athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution",
                       "athena:GetQueryResults", "athena:GetWorkGroup", "athena:BatchGetQueryExecution"],
                      sub("arn:${AWS::Partition}:athena:${AWS::Region}:${AWS::AccountId}:workgroup/${LabName}")),
                allow(["athena:GetDataCatalog"], sub("arn:${AWS::Partition}:athena:${AWS::Region}:${AWS::AccountId}:datacatalog/AwsDataCatalog")),
                allow(["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"], bucket_arn),
                allow(["s3:GetObject"], [sub("${LabBucket.Arn}/curated/*"), sub("${LabBucket.Arn}/results/*")]),
                allow(["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], sub("${LabBucket.Arn}/results/*")),
                allow(["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions", "glue:GetPartition", "glue:BatchGetPartition"],
                      [sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:catalog"),
                       sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:database/${DatabaseName}"),
                       sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:table/${DatabaseName}/orders")]),
            ])}]}},
        "Workflow": {"Type": "AWS::StepFunctions::StateMachine", "DependsOn": ["OrdersTable", "Workgroup"], "Properties": {
            "StateMachineName": ref("LabName"), "StateMachineType": "STANDARD", "RoleArn": attr("WorkflowRole"),
            "DefinitionString": json.dumps(definition),
            "DefinitionSubstitutions": {"FunctionArn": attr("Transformer"), "DatabaseName": ref("Database"), "WorkgroupName": ref("Workgroup")},
            "Tags": tags}},
    }
    return {"AWSTemplateFormatVersion": "2010-09-09", "Description": "Data Pipelines on AWS in 120 Minutes bounded lab",
            "Parameters": {"LabName": {"Type": "String", "AllowedPattern": "dp120-[a-f0-9]{12}"},
                           "DatabaseName": {"Type": "String", "AllowedPattern": "dp120_[a-f0-9]{12}"}},
            "Resources": resources,
            "Outputs": {"Bucket": {"Value": ref("LabBucket")}, "FunctionName": {"Value": ref("Transformer")},
                        "WorkflowArn": {"Value": ref("Workflow")}, "DatabaseName": {"Value": ref("Database")},
                        "Workgroup": {"Value": ref("Workgroup")}}}


if __name__ == "__main__":
    target = HERE / "build/template.json"
    target.parent.mkdir(exist_ok=True)
    target.write_text(json.dumps(build(), indent=2))
    print("Built build/template.json from the local handler sources.")
