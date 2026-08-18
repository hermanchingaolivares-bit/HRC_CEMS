-- =========================================================
-- 001 | Motivos de rechazo de los catalogos
--
-- Los seis motivos de la fase 1 se pensaron para equipos y series. Al cargar
-- los catalogos aparecieron dos defectos que no encajan en ninguno:
--
--   NOMBRE_DUPLICADO  el mismo tipo o la misma unidad escrita dos veces
--                     ('Mesa Qx Avanzada' aparece repetida en INDICES Y COSTOS)
--   VALOR_INVALIDO    un numero que no se puede leer, o un indice de
--                     mantenimiento fuera de la escala 12-18 / 19 / 22
--
-- Sin esto, el ETL no puede dejar constancia de ellos: la restriccion los
-- rechaza y la carga completa se cae.
-- =========================================================

ALTER TABLE rechazo DROP CONSTRAINT rechazo_motivo_valido;

ALTER TABLE rechazo ADD CONSTRAINT rechazo_motivo_valido
    CHECK (motivo IN (
        'SERIE_VACIA', 'SERIE_DUPLICADA', 'SIN_CATASTRO', 'FECHA_INVALIDA',
        'SIN_PLAN', 'TIPO_DESCONOCIDO', 'NOMBRE_DUPLICADO', 'VALOR_INVALIDO'));

COMMENT ON CONSTRAINT rechazo_motivo_valido ON rechazo IS
    'Motivos que el ETL puede reportar. Ampliar aqui exige una migracion nueva.';
