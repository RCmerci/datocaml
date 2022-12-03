open Eio
open Buf_read
open Buf_read.Syntax
module StrMap = Map.Make (String)

let is_space c = List.mem c [ ' '; '\t'; '\n'; '\r'; '\012' ]

let profile_name =
  let+ name =
    skip_while is_space *> char '[' *> take_while1 (fun c -> c <> ']')
    <* char ']' <* skip_while is_space
  in
  let name = String.trim name in
  if String.starts_with ~prefix:"profile" name then
    let l = String.split_on_char ' ' name in
    List.nth l (List.length l - 1)
  else name

let access_key =
  string "aws_access_key_id"
  *> skip_while (fun c -> is_space c || c = '=')
  *> line

let secret_key =
  string "aws_secret_access_key"
  *> skip_while (fun c -> is_space c || c = '=')
  *> line

let one_profile =
  let* profile_name in
  let* access_key in
  let+ secret_key in
  (profile_name, (access_key, secret_key))

let kvs =
  seq
    ~stop:
      (let+ c = peek_char in
       c = Some '\n' || c = None)
    (let* k = take_while1 (fun c -> not (is_space c)) in
     let+ v = skip_while (fun c -> is_space c || c = '=') *> line in
     (k, v))

let one_config =
  let* profile_name in
  let+ kvs in
  (profile_name, List.of_seq kvs)

let get_credential ?(which = "default") env =
  let home_dir = Unix.getenv "HOME" in
  let ( / ) = Path.( / ) in
  let path = Stdenv.fs env / home_dir / ".aws" / "credentials" in
  Path.with_open_in path @@ fun flow ->
  let buf = of_flow ~max_size:200 flow in
  let all_profiles = (seq (one_profile <* skip_while is_space)) buf in
  let (m : (string * string) StrMap.t) = StrMap.of_seq all_profiles in
  StrMap.find_opt which m

let get_region ?(which = "default") env =
  let home_dir = Unix.getenv "HOME" in
  let ( / ) = Path.( / ) in
  let path = Stdenv.fs env / home_dir / ".aws" / "config" in
  Path.with_open_in path @@ fun flow ->
  let buf = of_flow ~max_size:200 flow in
  let all_configs = (seq (one_config <* skip_while is_space)) buf in
  let m = StrMap.of_seq all_configs in
  match StrMap.find_opt which m with
  | None -> None
  | Some l -> StrMap.of_seq (List.to_seq l) |> StrMap.find_opt "region"
