(* I just need sign_request and don't want to whole aws lib,
   so just copied needed modules and funcs.
*)

module Util = struct
  let drop_empty l =
    List.filter (fun p -> String.length (String.trim (fst p)) <> 0) l

  let or_error v err =
    match v with
    | None -> `Error err
    | Some v -> `Ok v

  let of_option default = function
    | None -> default
    | Some v -> v

  let of_option_exn = function
    | Some v -> v
    | None -> failwith "Expected Some v, got None."

  let rec list_find l key =
    match l with
    | [] -> None
    | (k, v) :: xs -> if k = key then Some v else list_find xs key

  let rec list_filter_opt = function
    | [] -> []
    | Some v :: xs -> v :: list_filter_opt xs
    | None :: xs -> list_filter_opt xs

  let option_bind o f =
    match o with
    | None -> None
    | Some v -> f v

  let option_map v f =
    match v with
    | None -> None
    | Some v -> Some (f v)

  let rec option_all = function
    | [] -> Some []
    | Some v :: xs -> option_bind (option_all xs) (fun rest -> Some (v :: rest))
    | None :: _ -> None
end

module Time = struct
  module C = CalendarLib.Calendar
  module P = CalendarLib.Printer.Calendar

  let date_yymmdd = P.sprint "%Y%m%d"

  let date_time_iso8601 = P.sprint "%Y-%m-%dT%H:%M:%S"

  let date_time = P.sprint "%Y%m%dT%H%M%SZ"

  let now_utc () = C.(now () |> to_gmt)

  (* (tmcgilchrist) This function is expecting datetimes like
      - "2021-03-17T21:43:32.000Z" from EC2 or
      - "2021-03-18T09:38:33Z" from STS
     We regex off the trailing ".000" and parse them. If there are other
     datetime formats in xml / json there will be trouble and the parser
     will fail with xml node not present or json attribute not present.
  *)
  let parse s =
    P.from_fstring "%Y-%m-%dT%TZ"
      (Str.replace_first (Str.regexp "\\.\\([0-9][0-9][0-9]\\)") "" s)

  let format t = P.sprint "%Y-%m-%dT%T.000Z" t
end

module Hash = struct
  let _sha256 ?key str =
    match key with
    | Some key -> Digestif.SHA256.hmac_string ~key str
    | None -> Digestif.SHA256.digest_string str

  let sha256 ?key str = _sha256 ?key str |> Digestif.SHA256.to_raw_string

  let sha256_hex ?key str = _sha256 ?key str |> Digestif.SHA256.to_hex

  let sha256_base64 ?key str = Base64.encode_string @@ sha256 ?key str
end

module Request = struct
  type meth =
    [ `DELETE
    | `GET
    | `HEAD
    | `OPTIONS
    | `CONNECT
    | `TRACE
    | `Other of string
    | `PATCH
    | `POST
    | `PUT
    ]

  let string_of_meth = function
    | `DELETE -> "DELETE"
    | `GET -> "GET"
    | `HEAD -> "HEAD"
    | `OPTIONS -> "OPTIONS"
    | `CONNECT -> "CONNECT"
    | `TRACE -> "TRACE"
    | `Other s -> s
    | `PATCH -> "PATCH"
    | `POST -> "POST"
    | `PUT -> "PUT"

  type headers = (string * string) list

  type signature_version =
    | V4
    | V2
    | S3

  type t = meth * Uri.t * headers
end

let encode_query ps =
  (* NOTE(dbp 2015-03-13): We want just:
     A-Z, a-z, 0-9, hyphen ( - ), underscore ( _ ), period ( . ), and tilde ( ~ ).
            As per the docs:
            http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
            Uri has that as it's fall-through, which at least currently (and hopefully forever)
            ~component:`Authority causes it to use.
  *)
  let encoded =
    List.map
      (fun (k, v) ->
        let key = Uri.pct_encode ~component:`Authority k in
        let value =
          match v with
          | [] -> ""
          | [ x ] -> Uri.pct_encode ~component:`Authority x
          | _ -> failwith "AWS query cannot have multiple values for same key"
        in
        (key, value))
      ps
  in
  let sorted = List.sort (fun a b -> compare (fst a) (fst b)) encoded in
  let joined = List.map (fun (k, v) -> k ^ "=" ^ v) sorted in
  String.concat "&" joined

let debug = ref false

let sign_request ~access_key ~secret_key ~service ~region
    (meth, uri, headers, request_paramters) =
  let host = Util.of_option_exn (Endpoints.endpoint_of service region) in
  let params = encode_query (Uri.query uri) in
  let sign key msg = Hash.sha256 ~key msg in
  let get_signature_key key date region service =
    sign (sign (sign (sign ("AWS4" ^ key) date) region) service) "aws4_request"
  in
  let now = Time.now_utc () in
  let amzdate = Time.date_time now in
  let datestamp = Time.date_yymmdd now in
  let canonical_uri = "/" in
  let canonical_querystring = params in
  let payload_hash = Hash.sha256_hex request_paramters in
  let content_type =
    List.find (fun (k, _) -> String.lowercase_ascii k = "content-type") headers
    |> snd |> String.lowercase_ascii
  in
  let amz_target =
    List.find (fun (k, _) -> String.lowercase_ascii k = "x-amz-target") headers
    |> snd
  in
  let canonical_headers =
    "content-type:" ^ content_type ^ "\nhost:" ^ host ^ "\n" ^ "x-amz-date:"
    ^ amzdate ^ "\nx-amz-target:" ^ amz_target ^ "\n"
  in
  let signed_headers = "content-type;host;x-amz-date;x-amz-target" in
  let canonical_request =
    Request.string_of_meth meth
    ^ "\n" ^ canonical_uri ^ "\n" ^ canonical_querystring ^ "\n"
    ^ canonical_headers ^ "\n" ^ signed_headers ^ "\n" ^ payload_hash
  in
  if !debug then Eio.traceln "canonical_request: %s" canonical_request;
  let algorithm = "AWS4-HMAC-SHA256" in
  let credential_scope =
    datestamp ^ "/" ^ region ^ "/" ^ service ^ "/" ^ "aws4_request"
  in
  let string_to_sign =
    algorithm ^ "\n" ^ amzdate ^ "\n" ^ credential_scope ^ "\n"
    ^ Hash.sha256_hex canonical_request
  in
  if !debug then Eio.traceln "string_to_sign: %s" string_to_sign;
  let signing_key = get_signature_key secret_key datestamp region service in
  let signature = Hash.sha256_hex ~key:signing_key string_to_sign in
  let authorization_header =
    String.concat ""
      [ algorithm
      ; " "
      ; "Credential="
      ; access_key
      ; "/"
      ; credential_scope
      ; ", "
      ; "SignedHeaders="
      ; signed_headers
      ; ", "
      ; "Signature="
      ; signature
      ]
  in
  let headers =
    ("x-amz-date", amzdate) (* :: ("x-amz-content-sha256", payload_hash) *)
    :: ("Authorization", authorization_header)
    :: headers
  in
  (meth, uri, headers)
