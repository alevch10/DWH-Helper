
### 1. Таблица `ym_raw_data`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_raw_data (
    watch_id                    NUMERIC(20,0),
    page_view_id                INTEGER,
    visit_id                    NUMERIC(20,0),
    counter_id                  INTEGER,
    client_id                   NUMERIC(20,0),
    counter_user_id_hash        NUMERIC(20,0),
    date_time                   TIMESTAMPTZ,
    title                       VARCHAR(800),
    goals_id                    TEXT,
    url                         VARCHAR(2048),
    referer                     VARCHAR(3000),
    utm_campaign                TEXT,
    utm_content                 VARCHAR(512),
    utm_medium                  VARCHAR(14),
    utm_source                  VARCHAR(50),
    utm_term                    VARCHAR(512),
    operating_system            VARCHAR(22),
    has_gclid                   BOOLEAN,
    gclid                       TEXT,
    last_traffic_source         VARCHAR(9),
    last_search_engine_root     VARCHAR(10),
    last_search_engine          VARCHAR(17),
    last_adv_engine             VARCHAR(14),
    last_social_network         VARCHAR(13),
    last_social_network_profile VARCHAR(50),
    recommendation_system       VARCHAR(10),
    messenger                   VARCHAR(8),
    browser                     VARCHAR(17),
    browser_major_version       INTEGER,
    browser_minor_version       INTEGER,
    browser_country             VARCHAR(2),
    browser_engine              VARCHAR(50),
    browser_engine_version1     SMALLINT,
    browser_engine_version2     SMALLINT,
    browser_engine_version3     SMALLINT,
    browser_engine_version4     SMALLINT,
    browser_language            VARCHAR(4),
    cookie_enabled              BOOLEAN,
    device_category             VARCHAR(1),
    javascript_enabled          BOOLEAN,
    mobile_phone                VARCHAR(14),
    mobile_phone_model          VARCHAR(50),
    operating_system_root       VARCHAR(20),
    physical_screen_height      SMALLINT,
    physical_screen_width       SMALLINT,
    screen_colors               SMALLINT,
    screen_format               VARCHAR(9),
    screen_height               SMALLINT,
    screen_orientation          SMALLINT,
    screen_orientation_name     VARCHAR(9),
    screen_width                SMALLINT,
    window_client_height        SMALLINT,
    window_client_width         SMALLINT,
    ip_address                  VARCHAR(24),
    region_city                 VARCHAR(26),
    region_country              VARCHAR(63),
    is_page_view                BOOLEAN,
    is_turbo_app                BOOLEAN,
    iframe                      BOOLEAN,
    link                        BOOLEAN,
    download                    BOOLEAN,
    not_bounce                  BOOLEAN,
    artificial                  BOOLEAN,
    offline_call_talk_duration  SMALLINT,
    offline_call_hold_duration  SMALLINT,
    offline_call_missed         SMALLINT,
    offline_call_tag            VARCHAR(1000),
    offline_call_first_time_caller SMALLINT,
    offline_call_url            VARCHAR(2048),
    offline_uploading_id        VARCHAR(10),
    params                      VARCHAR(2048),
    http_error                  SMALLINT,
    network_type                VARCHAR(8),
    share_service               TEXT,
    share_url                   VARCHAR(2048),
    share_title                 TEXT,
    has_sbclid                  BOOLEAN,
    sbclid                      TEXT
);

CREATE INDEX idx_raw_visit_id     ON yandex_metrika.ym_raw_data (visit_id);
CREATE INDEX idx_raw_client_id    ON yandex_metrika.ym_raw_data (client_id);
CREATE INDEX idx_raw_date_time    ON yandex_metrika.ym_raw_data (date_time);
CREATE INDEX idx_raw_url_booking  ON yandex_metrika.ym_raw_data (url) WHERE url LIKE '%booking.avaclinic.ru%';
```

### 2. Таблица `ym_ad_data`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_ad_data (
    watch_id                    NUMERIC(20,0),
    page_view_id                INTEGER,
    visit_id                    NUMERIC(20,0),
    client_id                   NUMERIC(20,0),
    date_time                   TIMESTAMPTZ,
    url                         VARCHAR(2048),
    referer                     VARCHAR(3000),
    utm_campaign                TEXT,
    utm_content                 VARCHAR(512),
    utm_medium                  VARCHAR(14),
    utm_source                  VARCHAR(50),
    utm_term                    VARCHAR(512),
    last_traffic_source         VARCHAR(9),
    last_search_engine_root     VARCHAR(10),
    last_search_engine          VARCHAR(17),
    last_adv_engine             VARCHAR(14),
    last_social_network         VARCHAR(13),
    last_social_network_profile VARCHAR(50),
    recommendation_system       VARCHAR(10),
    messenger                   VARCHAR(8)
);

CREATE INDEX idx_ad_client_id ON yandex_metrika.ym_ad_data (client_id);
CREATE INDEX idx_ad_date_time ON yandex_metrika.ym_ad_data (date_time);
```

### 3. Таблица `ym_successful_entries`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_successful_entries (
    visit_id    NUMERIC(20,0),
    client_id   NUMERIC(20,0),
    date_time   TIMESTAMPTZ
);

CREATE INDEX idx_success_client_id ON yandex_metrika.ym_successful_entries (client_id);
CREATE INDEX idx_success_visit_id  ON yandex_metrika.ym_successful_entries (visit_id);
CREATE INDEX idx_success_date_time ON yandex_metrika.ym_successful_entries (date_time);
```

### 4. Таблица `ym_booking_visits`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_booking_visits (
    visit_id                NUMERIC(20,0) PRIMARY KEY,
    client_id               NUMERIC(20,0) NOT NULL,
    visit_start_time        TIMESTAMPTZ NOT NULL,
    utm_source              VARCHAR(50),
    utm_medium              VARCHAR(14),
    utm_campaign            TEXT,
    utm_term                VARCHAR(512),
    utm_content             VARCHAR(512),
    referer                 VARCHAR(3000),
    last_traffic_source     VARCHAR(9),
    device_category         VARCHAR(1),
    region_city             VARCHAR(26),
    had_successful_entry    BOOLEAN DEFAULT FALSE,
);

COMMENT ON TABLE yandex_metrika.ym_booking_visits IS 'Визиты на домен booking. Содержит информацию об источнике визита (UTM, last_traffic_source) и флаг записи. Используется для last-click атрибуции.';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.visit_id IS 'Идентификатор визита';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.client_id IS 'Анонимный идентификатор пользователя';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.visit_start_time IS 'Время начала визита (первый хит)';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.utm_source IS 'UTM Source';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.utm_medium IS 'UTM Medium';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.utm_campaign IS 'UTM Campaign';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.utm_term IS 'UTM Term';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.utm_content IS 'UTM Content';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.referer IS 'URL реферера (откуда пришёл пользователь перед визитом)';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.last_traffic_source IS 'Тип источника трафика: direct, internal, referral, organic, ad, saved, undefined, email, social, messenger, qrcode, recommend';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.device_category IS 'Тип устройства: 1-десктоп, 2-мобильный, 3-планшет, 4-TV';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.region_city IS 'Город пользователя (английское название)';
COMMENT ON COLUMN yandex_metrika.ym_booking_visits.had_successful_entry IS 'True, если в этом визите была успешная запись (событие confirmed)';

CREATE INDEX idx_booking_visits_client ON yandex_metrika.ym_booking_visits (client_id);
CREATE INDEX idx_booking_visits_start_time ON yandex_metrika.ym_booking_visits (visit_start_time);
CREATE INDEX idx_booking_visits_traffic_source ON yandex_metrika.ym_booking_visits (last_traffic_source);
```

### 5. Таблица `ym_booking_transitions`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_booking_transitions (
    id              BIGSERIAL PRIMARY KEY,
    client_id       NUMERIC(20,0) NOT NULL,
    hit_time        TIMESTAMPTZ NOT NULL,
    booking_url     VARCHAR(2048) NOT NULL,
    prev_url        VARCHAR(2048),
    transition_type VARCHAR(20) NOT NULL,
    last_traffic_source VARCHAR(9)
);

COMMENT ON TABLE yandex_metrika.ym_booking_transitions IS 'Переходы на booking: с каких страниц сайта или прямые входы. Содержит URL перехода и предыдущий URL.';
COMMENT ON COLUMN yandex_metrika.ym_booking_transitions.client_id IS 'Анонимный идентификатор пользователя';
COMMENT ON COLUMN yandex_metrika.ym_booking_transitions.hit_time IS 'Время перехода на booking';
COMMENT ON COLUMN yandex_metrika.ym_booking_transitions.booking_url IS 'URL страницы на booking, на которую перешли';
COMMENT ON COLUMN yandex_metrika.ym_booking_transitions.prev_url IS 'Предыдущий URL (если переход был с сайта avaclinic.ru)';
COMMENT ON COLUMN yandex_metrika.ym_booking_transitions.transition_type IS 'Тип перехода: from_site (с сайта) или direct_booking (прямой вход)';
COMMENT ON COLUMN yandex_metrika.ym_booking_transitions.last_traffic_source IS 'Тип источника трафика для визита, в котором произошёл переход (может быть полезно для анализа прямых входов)';

CREATE INDEX idx_booking_transitions_client_time ON yandex_metrika.ym_booking_transitions (client_id, hit_time);
CREATE INDEX idx_booking_transitions_type ON yandex_metrika.ym_booking_transitions (transition_type);
```

### 6. Таблица `ym_user_paths`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_user_paths (
    id                      BIGSERIAL PRIMARY KEY,
    client_id               NUMERIC(20,0),
    visit_id                NUMERIC(20,0),
    visit_start_time        TIMESTAMPTZ,
    visit_number            INTEGER,
    had_successful_entry    BOOLEAN DEFAULT FALSE,
    entry_time              TIMESTAMPTZ,
    utm_campaign                TEXT,
    utm_content                 VARCHAR(512),
    utm_medium                  VARCHAR(14),
    utm_source                  VARCHAR(50)
);

CREATE INDEX idx_paths_client      ON yandex_metrika.ym_user_paths (client_id);
CREATE INDEX idx_paths_entry_time  ON yandex_metrika.ym_user_paths (entry_time);
CREATE INDEX idx_paths_visit_num   ON yandex_metrika.ym_user_paths (client_id, visit_number);
```

### 7. Таблица `ym_call_data`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_call_data (
    visit_id                    NUMERIC(20,0),
    client_id                   NUMERIC(20,0),
    date_time                   TIMESTAMPTZ,
    offline_call_talk_duration  SMALLINT,
    offline_call_hold_duration  SMALLINT,
    offline_call_missed         SMALLINT,
    offline_call_tag            VARCHAR(1000),
    offline_call_first_time_caller SMALLINT,
    offline_call_url            VARCHAR(2048),
    offline_uploading_id        VARCHAR(10)
);

CREATE INDEX idx_call_visit_id     ON yandex_metrika.ym_call_data (visit_id);
CREATE INDEX idx_call_client_id    ON yandex_metrika.ym_call_data (client_id);
```


### 8. Таблица `ym_page_transitions`

```sql
CREATE TABLE IF NOT EXISTS yandex_metrika.ym_page_transitions (
    id              BIGSERIAL PRIMARY KEY,
    visit_id        NUMERIC(20,0) NOT NULL,
    client_id       NUMERIC(20,0) NOT NULL,
    transition_date    DATE NOT NULL,
    source          TEXT NOT NULL,
    target          TEXT NOT NULL,
    sequence_num    INTEGER NOT NULL
);

CREATE INDEX idx_transitions_visit ON yandex_metrika.ym_page_transitions (visit_id);
CREATE INDEX idx_transitions_date ON yandex_metrika.ym_page_transitions (date);
COMMENT ON TABLE yandex_metrika.ym_page_transitions IS 'Переходы между страницами внутри визита для построения sankey diagram';
```
