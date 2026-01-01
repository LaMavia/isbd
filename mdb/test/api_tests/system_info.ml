open! Common
open! Web.Models

let test (module S : ServerHandler) =
  ( __MODULE__
  , [ case ~name:"Hello" (fun _ ->
        let%lwt _ =
          S.request
            (module SystemInformation)
            (module Error)
            ~meth:`GET
            ~path:"/system/info"
            ()
        in
        (* Alcotest.(check' Testable.status) ~msg:"Succesful" ~expected:`OK ~actual:status; *)
        (* let body = body_res |> Utils.Unwrap.result ~exc:Error.web_error in *)
        (* let open Alcotest in *)
        (* check' *)
        (*   string *)
        (*   ~msg:"Proper author" *)
        (*   ~expected:"Zuzanna Surowiec" *)
        (*   ~actual:body.author; *)
        Lwt.return ())
    ] )
;;
