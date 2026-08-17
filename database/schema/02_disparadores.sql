-- =========================================================
-- HRC-CEMS | Disparadores
--
-- Dos automatismos:
--   1. actualizado_en se pone solo al modificar una fila
--   2. cada campo que cambia de valor deja una fila en "cambio"
--
-- El registro de cambios lo escribe el motor, no la aplicacion:
-- si dependiera de que alguien se acuerde, tarde o temprano se olvida.
--
-- El ETL puede marcar su origen antes de escribir:
--     SET LOCAL hrc.origen   = 'ETL';
--     SET LOCAL hrc.carga_id = '123';
-- Sin eso, el cambio queda registrado como MANUAL.
-- =========================================================


-- ---------------------------------------------------------
-- 1. Marca de tiempo de la ultima modificacion
-- ---------------------------------------------------------

CREATE OR REPLACE FUNCTION tocar_actualizado_en()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.actualizado_en := now();
    RETURN NEW;
END;
$$;


CREATE TRIGGER equipo_tocar_actualizado_en
    BEFORE UPDATE ON equipo
    FOR EACH ROW
    EXECUTE FUNCTION tocar_actualizado_en();


-- ---------------------------------------------------------
-- 2. Registro de cambios
--
-- Generica: sirve para cualquier tabla. Recibe como argumento
-- el nombre de su clave primaria.
-- ---------------------------------------------------------

CREATE OR REPLACE FUNCTION registrar_cambio()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    fila_anterior jsonb := to_jsonb(OLD);
    fila_nueva    jsonb := to_jsonb(NEW);
    nombre_pk     text  := TG_ARGV[0];
    campo         text;
    origen_actual text;
    carga_actual  integer;
BEGIN
    origen_actual := COALESCE(current_setting('hrc.origen', true), 'MANUAL');
    carga_actual  := NULLIF(current_setting('hrc.carga_id', true), '')::integer;

    FOR campo IN SELECT jsonb_object_keys(fila_nueva)
    LOOP
        -- La marca de tiempo cambia en cada UPDATE: registrarla seria solo ruido.
        CONTINUE WHEN campo = 'actualizado_en';

        IF fila_anterior -> campo IS DISTINCT FROM fila_nueva -> campo THEN
            INSERT INTO cambio (
                tabla, registro_id, campo, valor_anterior, valor_nuevo, origen, carga_id
            )
            VALUES (
                TG_TABLE_NAME,
                (fila_nueva ->> nombre_pk)::integer,
                campo,
                fila_anterior ->> campo,
                fila_nueva    ->> campo,
                origen_actual,
                carga_actual
            );
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$;


CREATE TRIGGER equipo_registrar_cambio
    AFTER UPDATE ON equipo
    FOR EACH ROW
    EXECUTE FUNCTION registrar_cambio('id_equipo');

CREATE TRIGGER tipo_equipo_registrar_cambio
    AFTER UPDATE ON tipo_equipo
    FOR EACH ROW
    EXECUTE FUNCTION registrar_cambio('id_tipo_equipo');

CREATE TRIGGER plan_mantenimiento_registrar_cambio
    AFTER UPDATE ON plan_mantenimiento
    FOR EACH ROW
    EXECUTE FUNCTION registrar_cambio('id_plan_mantenimiento');

CREATE TRIGGER hoja_de_vida_registrar_cambio
    AFTER UPDATE ON hoja_de_vida
    FOR EACH ROW
    EXECUTE FUNCTION registrar_cambio('id_hoja_de_vida');

CREATE TRIGGER orden_trabajo_registrar_cambio
    AFTER UPDATE ON orden_trabajo
    FOR EACH ROW
    EXECUTE FUNCTION registrar_cambio('id_orden_trabajo');

CREATE TRIGGER falla_registrar_cambio
    AFTER UPDATE ON falla
    FOR EACH ROW
    EXECUTE FUNCTION registrar_cambio('id_falla');
