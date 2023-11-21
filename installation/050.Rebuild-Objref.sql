begin
    pkg_objref.validate_rebuild_id_pkg_and_types();
    pkg_objref.rebuild_id_pkg_and_types();
    dbms_output.put_line('Object references were rebuilt');
end;