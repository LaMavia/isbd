open Lib

let () =
  let module TestSerializer =
    Serializer.Make (Cursor.StringCursor) (Cursor.StringCursor)
  in
  let module TestDeserializer =
    Deserializer.Make (Cursor.StringCursor) (Cursor.StringCursor)
  in
  let output_cursor = Cursor.StringCursor.create "" |> Result.get_ok in
  TestSerializer.write_columns
    [
      ("hello", `ColInt);
      ("there", `ColString);
      ("my", `ColInt);
      ("friend", `ColString);
    ]
    output_cursor;
  let bts = output_cursor |> Cursor.StringCursor.to_bytes in

  Utils.Debugging.print_hex_bytes "OUTPUT" bts;
  flush_all ();
  Printf.eprintf "\n=========================================\n\n";
  output_cursor |> Cursor.StringCursor.seek 0 |> ignore;
  TestDeserializer.read_columns output_cursor
  |> List.iter (fun (s, t) ->
         Printf.eprintf "%s\t%s\n" s
           (match t with `ColInt -> "INT" | `ColString -> "VCHAR"))

(* let usage_msg = "mdb [-s] [-d] <file> ..." in *)
(* let mode = ref Serialize and input_files = ref [] in *)
(* let anon_fun filename = input_files := filename :: !input_files in *)
(* let make_set_mode m = Arg.Unit (fun () -> (mode := m) |> ignore) in *)
(* let speclist = *)
(*   [ *)
(*     ("-s", make_set_mode Serialize, "serialize"); *)
(*     ("-d", make_set_mode Deserialize, "deserialize"); *)
(*   ] *)
(* in *)
(* Memtrace.trace_if_requested (); *)
(* Arg.parse speclist anon_fun usage_msg; *)
(* let a = Bigarray.Genarray.create Char C_layout [| 1; 1000 |] *)
(* and n0 = 5000000L in *)
(* UnsignedVLQSig.deserialize a () n0 (0, 0) |> ignore; *)
(* let n, off = UnsignedVLQSig.serialize a (0, 0) () |> Result.get_ok in *)
(* Printf.printf "%Ld \t (%d, %d)\n" n (fst off) (snd off) *)

(* let a =  / *)
(* let a = ManagedMMap.of_path 1 (List.hd !input_files) in *)
(* match !mode with *)
(* | Serialize -> ( *)
(*     match SignedVLQSig.serialize a.array (0, 0) with *)
(*     | Result.Ok (n, offset) -> *)
(*         Printf.printf "n=%Ld, offset=(%d, %d)\n" n (fst offset) (snd offset) *)
(*     | Result.Error e -> *)
(*         print_string e.reason *)
(*         (* match PersonEphSeqSig.serialize a 0 with *) *)
(*         (* | Result.Ok (ps, _) -> *) *)
(*         (*     Seq.iteri *) *)
(*         (*       (fun i p' -> *) *)
(*         (*         Printf.printf "%02d. person { age=%d; name=%s; surname=%s }\n" *) *)
(*         (*           (i + 1) p'.age p'.name p'.surname) *) *)
(*         (*       ps *) *)
(*         (* | Result.Error e -> print_string e.reason *)) *)
(* | Deserialize -> *)
(*     let n = -1002007L in *)
(*     SignedVLQSig.deserialize a.array n (0, 0) |> ignore *)
(* let person_dispenser len = *)
(*   let i = ref 0 in *)
(*   fun () -> *)
(*     if !i >= len then Option.None *)
(*     else ( *)
(*       i := !i + 1; *)
(*       Option.Some { age = 23; name = "Zuzanna"; surname = "Surowiec" }) *)
(* in *)
(* PersonEphSeqSig.deserialize a *)
(*   (Seq.of_dispenser @@ person_dispenser 50000) *)
(*   0 *)
(* |> ignore *)

(* let () = *)
(*   let open Openapi in *)
(*   let id_schema = *)
(*     Schema.(SchemaString { type_ = `String; default = Option.None }) *)
(*   in *)
(*   let page_public = *)
(*     Response. *)
(*       { *)
(*         content = *)
(*           MediaTypeMap.of_list *)
(*             [ ("id", MediaType.{ schema = Schema.to_ref "id" }) ]; *)
(*         description = "decription"; *)
(*       } *)
(*   in *)
(*   to_yojson *)
(*     { *)
(*       openapi = "3.1"; *)
(*       info = *)
(*         Info. *)
(*           { *)
(*             title = "title"; *)
(*             summary = "summary"; *)
(*             description = "description"; *)
(*             version = "1.0.0"; *)
(*           }; *)
(*       tags = []; *)
(*       components = *)
(*         Components. *)
(*           { *)
(*             schemas = SchemaMap.of_list [ ("id", id_schema) ]; *)
(*             responses = ResponseMap.of_list [ ("page_public", page_public) ]; *)
(*             requests = RequestMap.of_list []; *)
(*           }; *)
(*       paths = *)
(*         StringMap.of_list *)
(*           [ *)
(*             ( "/page/:page_id/", *)
(*               Path. *)
(*                 { *)
(*                   summary = "summary"; *)
(*                   description = "description"; *)
(*                   get = *)
(*                     Option.some *)
(*                       Operation. *)
(*                         { *)
(*                           description = "description"; *)
(*                           tags = []; *)
(*                           summary = "summary"; *)
(*                           operationId = "get_page_by_id"; *)
(*                           parameters = []; *)
(*                           requestBody = Reference.{ ref_ = "" }; *)
(*                           responses = StringMap.of_list []; *)
(*                         }; *)
(*                   put = Option.None; *)
(*                   post = Option.None; *)
(*                   delete = Option.None; *)
(*                   patch = Option.None; *)
(*                 } ); *)
(*           ]; *)
(*     } *)
(*   |> Yojson.Safe.pretty_to_string |> Printf.printf "%s\n" *)
