let endpoint_of svc_name region =
  match svc_name with
  | "dynamodb" -> (
    match region with
    | "ap-northeast-1" -> Some "dynamodb.ap-northeast-1.amazonaws.com"
    | "ap-northeast-2" -> Some "dynamodb.ap-northeast-2.amazonaws.com"
    | "ap-south-1" -> Some "dynamodb.ap-south-1.amazonaws.com"
    | "ap-southeast-1" -> Some "dynamodb.ap-southeast-1.amazonaws.com"
    | "ap-southeast-2" -> Some "dynamodb.ap-southeast-2.amazonaws.com"
    | "ca-central-1" -> Some "dynamodb.ca-central-1.amazonaws.com"
    | "eu-central-1" -> Some "dynamodb.eu-central-1.amazonaws.com"
    | "eu-north-1" -> Some "dynamodb.eu-north-1.amazonaws.com"
    | "eu-west-1" -> Some "dynamodb.eu-west-1.amazonaws.com"
    | "eu-west-2" -> Some "dynamodb.eu-west-2.amazonaws.com"
    | "eu-west-3" -> Some "dynamodb.eu-west-3.amazonaws.com"
    | "local" -> Some "localhost:8000"
    | "sa-east-1" -> Some "dynamodb.sa-east-1.amazonaws.com"
    | "us-east-1" -> Some "dynamodb.us-east-1.amazonaws.com"
    | "us-east-2" -> Some "dynamodb.us-east-2.amazonaws.com"
    | "us-west-1" -> Some "dynamodb.us-west-1.amazonaws.com"
    | "us-west-2" -> Some "dynamodb.us-west-2.amazonaws.com"
    | _ -> None)
  | _ -> None

let url_of svc_name region =
  match endpoint_of svc_name region with
  | Some var -> Some ("https://" ^ var)
  | None -> None
