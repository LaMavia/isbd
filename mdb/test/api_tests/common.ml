open Utils.Ops
open Lib

module type Serializable = sig
  type t

  val t_of_yojson : Yojson.Safe.t -> t
  val yojson_of_t : t -> Yojson.Safe.t
end

module type ServerHandler = sig
  val request
    :  (module Serializable with type t = 'r)
    -> (module Serializable with type t = 'e)
    -> meth:Cohttp.Code.meth
    -> path:string
    -> ?body:Yojson.Safe.t
    -> unit
    -> (Cohttp.Code.status_code * ('r, 'e) result) Lwt.t

  val kill : unit -> unit
end

module type Server = sig
  val start : ?interface:string -> ?port:int -> unit -> (module ServerHandler)
end

module Testable = struct
  let data_record =
    let open Data in
    let pp_data_type ppf = function
      | `DataInt i -> Format.pp_print_int ppf @@ Int64.to_int i
      | `DataVarchar s -> Format.pp_print_string ppf s
      | `DataBool b -> Format.pp_print_bool ppf b
    in
    let pp_data_record ppf (d : data_record) = Format.pp_print_array pp_data_type ppf d in
    Alcotest.testable pp_data_record ( = )
  ;;

  let columns : Lib.Column.t array Alcotest.testable =
    let open Format in
    let pp_column ppf = function
      | name, `ColInt ->
        pp_print_string ppf name;
        print_space ();
        pp_print_string ppf "INT64"
      | name, `ColVarchar ->
        pp_print_string ppf name;
        print_space ();
        pp_print_string ppf "VARCHAR"
    in
    let pp_columns = Format.pp_print_array pp_column in
    Alcotest.testable pp_columns ( = )
  ;;

  let yojson = Alcotest.testable Yojson.Safe.pp Yojson.Safe.equal

  let status =
    let pp_status ppf status =
      Format.pp_print_string ppf (Cohttp.Code.string_of_status status)
    in
    Alcotest.testable pp_status ( = )
  ;;
end

let unique_table_name () = Printf.sprintf "table%s" Web.Core.Uuid.(v4 () |> to_string)

module MdbServer : Server = struct
  (* val start : ?interface:string -> ?port:int -> (module ServerHandler) *)
  let start ?(interface = "localhost") ?(port = 8080) () =
    let open Lwt in
    (* match Unix.fork () with *)
    (* | 0 -> *)
    (*   Dream.run ~interface ~port @@ Web.Main.app (); *)
    (*   while true do *)
    (*     () *)
    (*   done *)
    (* | server_pid -> *)
    let module MdbHandler : ServerHandler = struct
      (* val request *)
      (*   :  meth:Cohttp.Code.meth *)
      (*   -> path:string *)
      (*   -> body:Yojson.Safe.t *)
      (*   -> (Cohttp.Code.status_code * Yojson.Safe.t) Lwt.t *)
      let request
            (type r)
            (type e)
            (module Res : Serializable with type t = r)
            (module Err : Serializable with type t = e)
            ~meth
            ~path
            ?body
            ()
        =
        assert (String.starts_with ~prefix:"/" path);
        let open Cohttp in
        let open Cohttp_lwt in
        let open Cohttp_lwt_unix in
        let%lwt res, body =
          let url = Uri.make ~scheme:"http" ~host:interface ~port ~path () in
          Printf.eprintf "Calling %s\n%!" (Uri.to_string url);
          let ctx = Net.default_ctx in
          Client.call
            ~ctx
            ~headers:(Header.of_list [])
            ?body:(Option.map (Yojson.Safe.to_string @> Cohttp_lwt.Body.of_string) body)
            meth
            url
        in
        let%lwt body_json = Body.to_string body >|= Yojson.Safe.from_string in
        let status = res |> Response.status in
        let status_code = Code.code_of_status status in
        let response_body =
          if Cohttp.Code.is_error status_code
          then Error (Err.t_of_yojson body_json)
          else Ok (Res.t_of_yojson body_json)
        in
        return (status, response_body)
      ;;

      (* val kill : unit -> unit *)
      (* let kill () = Unix.kill server_pid Sys.sigkill *)
      let kill () = ()
    end
    in
    (module MdbHandler : ServerHandler)
  ;;
end
(**)
(* let free () = *)
(*   print_endline "freeing all resources"; *)
(*   Lwt.return () *)
(* ;; *)

let case ~name ?(speed = `Quick) f =
  let open Alcotest_lwt in
  test_case name speed
  @@ fun _switch () ->
  let test (type t) (module T : Serializable with type t = t) ~msg ~expected ~actual =
    Alcotest.check'
      Testable.yojson
      ~msg
      ~expected:(T.yojson_of_t expected)
      ~actual:(T.yojson_of_t actual)
  in
  (* Lwt_switch.add_hook (Some switch) free; *)
  f test
;;
