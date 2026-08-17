-- =========================================================
-- HRC-CEMS | Esquema de la base de datos
-- Motor: PostgreSQL 15 o superior
--   (usa UNIQUE ... NULLS NOT DISTINCT y columnas generadas)
--
-- Orden de aplicacion:
--   01_tablas.sql -> 02_disparadores.sql -> 03_vistas.sql
-- =========================================================


-- =========================================================
-- Capa 1 | Catalogos
-- =========================================================

CREATE TABLE tipo_equipo (
    id_tipo_equipo      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre              text     NOT NULL UNIQUE,
    categoria           text     NOT NULL,
    im_funcion          smallint,
    im_mantenimiento    smallint,
    im_riesgo_fisico    smallint,
    im_antecedentes     smallint,
    im_total            smallint GENERATED ALWAYS AS (
                            COALESCE(im_funcion, 0)
                          + COALESCE(im_mantenimiento, 0)
                          + COALESCE(im_riesgo_fisico, 0)
                          + COALESCE(im_antecedentes, 0)
                        ) STORED,
    vida_util_anios     smallint,

    CONSTRAINT tipo_equipo_categoria_valida
        CHECK (categoria IN ('CRITICO', 'RELEVANTE', 'IM_MAYOR_12'))
);

COMMENT ON TABLE  tipo_equipo IS
    'Tipo de equipo, no la unidad fisica. El indice de mantenimiento y la categoria son del tipo.';
COMMENT ON COLUMN tipo_equipo.im_total IS
    'Suma de los cuatro factores de Fennigkoh y Smith. La calcula la base, no se escribe a mano.';


CREATE TABLE servicio_clinico (
    id_servicio_clinico integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre              text    NOT NULL UNIQUE,
    responsable         text,
    anexo               text,
    correo              text
);

COMMENT ON TABLE servicio_clinico IS
    'Servicio del hospital donde esta el equipo. El contacto viene de la agenda de la unidad.';


CREATE TABLE proveedor (
    id_proveedor integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre       text    NOT NULL UNIQUE,
    rut          text,
    contacto     text
);


-- =========================================================
-- Capa 2 | Equipo
-- =========================================================

CREATE TABLE equipo (
    id_equipo           integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    serie               text    NOT NULL UNIQUE,
    nic                 text,
    n_inventario        text,
    tipo_equipo_id      integer NOT NULL REFERENCES tipo_equipo (id_tipo_equipo),
    marca               text,
    modelo              text,
    servicio_clinico_id integer REFERENCES servicio_clinico (id_servicio_clinico),
    sector              text,
    recinto             text,
    fecha_catastro      date,
    anio_adquisicion    smallint,
    origen              text,
    costo               numeric(14, 2),
    tenencia            text,
    equipo_padre_id     integer REFERENCES equipo (id_equipo),
    fecha_baja          date,
    motivo_baja         text,
    creado_en           timestamptz NOT NULL DEFAULT now(),
    actualizado_en      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT equipo_serie_no_vacia
        CHECK (btrim(serie) <> ''),
    CONSTRAINT equipo_tenencia_valida
        CHECK (tenencia IS NULL OR tenencia IN ('PROPIO', 'ARRIENDO', 'COMODATO')),
    CONSTRAINT equipo_no_es_su_propio_padre
        CHECK (equipo_padre_id IS DISTINCT FROM id_equipo),
    CONSTRAINT equipo_anio_adquisicion_valido
        CHECK (anio_adquisicion IS NULL OR anio_adquisicion BETWEEN 1900 AND 2100)
);

COMMENT ON TABLE  equipo IS
    'La unidad fisica. La serie es el identificador visible; id_equipo amarra el historial.';
COMMENT ON COLUMN equipo.serie IS
    'Identificador visible. Normalizada con recorte y mayusculas por el ETL.';
COMMENT ON COLUMN equipo.nic IS
    'Opcional y manual. El sistema nunca lo genera ni lo deduce.';
COMMENT ON COLUMN equipo.fecha_baja IS
    'Si tiene valor, el equipo esta de baja. El estado no se guarda aparte: se deduce de aqui.';

CREATE INDEX equipo_tipo_equipo_idx      ON equipo (tipo_equipo_id);
CREATE INDEX equipo_servicio_clinico_idx ON equipo (servicio_clinico_id);
CREATE INDEX equipo_padre_idx            ON equipo (equipo_padre_id);
CREATE INDEX equipo_nic_idx              ON equipo (nic) WHERE nic IS NOT NULL;


-- =========================================================
-- Capa 3 | Historia
-- =========================================================

CREATE TABLE plan_mantenimiento (
    id_plan_mantenimiento integer  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    equipo_id             integer  NOT NULL REFERENCES equipo (id_equipo),
    anio                  smallint NOT NULL,
    semestre              smallint,
    frecuencia_anual      numeric(3, 1),
    mes_programado        smallint,
    ejecutor              text,
    proveedor_id          integer  REFERENCES proveedor (id_proveedor),
    estado                text,
    situacion             text,
    fecha_ejecucion       date,

    CONSTRAINT plan_semestre_valido
        CHECK (semestre IS NULL OR semestre IN (1, 2)),
    CONSTRAINT plan_mes_valido
        CHECK (mes_programado IS NULL OR mes_programado BETWEEN 1 AND 12),
    CONSTRAINT plan_ejecutor_valido
        CHECK (ejecutor IS NULL OR ejecutor IN (
            'INTERNO', 'EXTERNO', 'CONTRATO', 'CONVENIO', 'COMODATO', 'GARANTIA')),
    CONSTRAINT plan_situacion_valida
        CHECK (situacion IS NULL OR situacion IN (
            'EJECUTADO', 'EJECUTADO_CO', 'NE_REPROGRAMACION', 'NE_NO_OPERATIVO',
            'NE_SUGERIDO_BAJA', 'NE_SERVICIO_TECNICO', 'NE_NO_ENCONTRADO')),
    CONSTRAINT plan_una_programacion_por_mes
        UNIQUE NULLS NOT DISTINCT (equipo_id, anio, mes_programado)
);

COMMENT ON TABLE  plan_mantenimiento IS
    'Una fila por mantenimiento programado. Define el universo de equipos del sistema.';
COMMENT ON COLUMN plan_mantenimiento.fecha_ejecucion IS
    'Opcional: el plan de IM no la registra. El cumplimiento se calcula al consultar.';

CREATE INDEX plan_equipo_anio_idx ON plan_mantenimiento (equipo_id, anio);
CREATE INDEX plan_anio_idx        ON plan_mantenimiento (anio);


CREATE TABLE hoja_de_vida (
    id_hoja_de_vida     integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    equipo_id           integer NOT NULL REFERENCES equipo (id_equipo),
    fecha               date    NOT NULL,
    tipo                text    NOT NULL,
    documento           text,
    reporte             text,
    servicio_clinico_id integer REFERENCES servicio_clinico (id_servicio_clinico),
    responsable         text,
    costo               numeric(14, 2),
    garantia            boolean,
    respaldo            text,
    fuente              text,

    CONSTRAINT hoja_de_vida_tipo_valido
        CHECK (tipo IN (
            'ENTREGA', 'PRESTAMO', 'SALIDA_SERVICIO_TECNICO', 'INFORME_TECNICO',
            'CAPACITACION', 'MANT_PREVENTIVO', 'MANT_CORRECTIVO', 'INSTALACION',
            'RECEPCION')),
    CONSTRAINT hoja_de_vida_sin_duplicados
        UNIQUE NULLS NOT DISTINCT (equipo_id, fecha, documento, tipo)
);

COMMENT ON TABLE  hoja_de_vida IS
    'Historial de intervenciones del equipo. Todas juntas: una fecha, un tipo, un documento y un texto.';
COMMENT ON CONSTRAINT hoja_de_vida_sin_duplicados ON hoja_de_vida IS
    'Reemplaza al id_unico del prototipo: volver a cargar la misma fuente no duplica el historial.';

CREATE INDEX hoja_de_vida_equipo_fecha_idx ON hoja_de_vida (equipo_id, fecha DESC);
CREATE INDEX hoja_de_vida_tipo_idx         ON hoja_de_vida (tipo);


CREATE TABLE orden_trabajo (
    id_orden_trabajo    integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo              text    NOT NULL UNIQUE,
    equipo_id           integer REFERENCES equipo (id_equipo),
    equipo_texto        text,
    servicio_clinico_id integer REFERENCES servicio_clinico (id_servicio_clinico),
    fecha_apertura      date,
    fecha_cierre        date,
    problema            text,
    trabajo             text,
    estado              text,
    solicitante         text,
    responsable         text,

    CONSTRAINT orden_trabajo_cierre_posterior
        CHECK (fecha_cierre IS NULL OR fecha_apertura IS NULL
               OR fecha_cierre >= fecha_apertura)
);

COMMENT ON COLUMN orden_trabajo.equipo_id IS
    'Puede quedar vacio: hay trabajos que no se asocian a ninguna serie. Ese registro no se pierde.';
COMMENT ON COLUMN orden_trabajo.equipo_texto IS
    'Lo que escribio la unidad cuando no hay serie, para poder asociarlo despues.';

CREATE INDEX orden_trabajo_equipo_idx   ON orden_trabajo (equipo_id);
CREATE INDEX orden_trabajo_servicio_idx ON orden_trabajo (servicio_clinico_id);
CREATE INDEX orden_trabajo_apertura_idx ON orden_trabajo (fecha_apertura DESC);


CREATE TABLE falla (
    id_falla      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    equipo_id     integer REFERENCES equipo (id_equipo),
    fecha         date,
    reporte       text,
    severidad     smallint,
    ocurrencia    smallint,
    impacto       smallint,
    rpn           integer GENERATED ALWAYS AS (
                      COALESCE(severidad, 0) * COALESCE(ocurrencia, 0) * COALESCE(impacto, 0)
                  ) STORED,
    criticidad    text,
    estado        text,
    cotizacion    text,
    costo         numeric(14, 2),
    informe       text,
    observaciones text,

    CONSTRAINT falla_criticidad_valida
        CHECK (criticidad IS NULL OR criticidad IN (
            'PRIORITARIA', 'MODERADA', 'BAJA', 'MUY_BAJA')),
    CONSTRAINT falla_estado_valido
        CHECK (estado IS NULL OR estado IN (
            'SIN_GESTIONAR', 'OT_GENERADA', 'COTIZACION_SOLICITADA', 'COTIZACION_RECIBIDA',
            'SEM_REALIZADA', 'OC_EMITIDA', 'SERVICIO_COORDINADO', 'SUBSANADO',
            'DADO_DE_BAJA', 'DESESTIMADO'))
);

COMMENT ON COLUMN falla.rpn IS
    'Severidad x Ocurrencia x Impacto. El Impacto reemplaza a la Deteccion del AMFE clasico.';

CREATE INDEX falla_equipo_idx ON falla (equipo_id);


-- =========================================================
-- Capa 4 | Control
-- =========================================================

CREATE TABLE carga (
    id_carga         integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha            timestamptz NOT NULL DEFAULT now(),
    fuente           text        NOT NULL,
    filas_leidas     integer,
    filas_cargadas   integer,
    filas_rechazadas integer
);

COMMENT ON TABLE carga IS 'Una fila por corrida del ETL.';


CREATE TABLE rechazo (
    id_rechazo integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    carga_id   integer REFERENCES carga (id_carga),
    fuente     text,
    motivo     text NOT NULL,
    valor      text,
    detalle    text,

    CONSTRAINT rechazo_motivo_valido
        CHECK (motivo IN (
            'SERIE_VACIA', 'SERIE_DUPLICADA', 'SIN_CATASTRO', 'FECHA_INVALIDA',
            'SIN_PLAN', 'TIPO_DESCONOCIDO'))
);

COMMENT ON TABLE rechazo IS
    'Lo que el ETL descarto y por que. Es la lista de correcciones pendientes en las planillas.';

CREATE INDEX rechazo_carga_idx  ON rechazo (carga_id);
CREATE INDEX rechazo_motivo_idx ON rechazo (motivo);


CREATE TABLE cambio (
    id_cambio      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tabla          text        NOT NULL,
    registro_id    integer     NOT NULL,
    campo          text        NOT NULL,
    valor_anterior text,
    valor_nuevo    text,
    fecha_cambio   timestamptz NOT NULL DEFAULT now(),
    origen         text        NOT NULL DEFAULT 'MANUAL',
    carga_id       integer     REFERENCES carga (id_carga),

    CONSTRAINT cambio_origen_valido
        CHECK (origen IN ('ETL', 'MANUAL'))
);

COMMENT ON TABLE cambio IS
    'Registro de modificaciones: una fila por campo que cambia de valor. La llenan los disparadores.';

CREATE INDEX cambio_registro_idx ON cambio (tabla, registro_id);
CREATE INDEX cambio_fecha_idx    ON cambio (fecha_cambio DESC);
