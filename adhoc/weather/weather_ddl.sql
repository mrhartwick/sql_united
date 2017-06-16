create table diap01.mec_us_united_20056.ual_freq_slm_tbl1
(
    user_id        varchar(50) not null,
    conversiontime int         not null,
    impressiontime int         not null,
    imp_key varchar not null,
    revenue        decimal     not null,
    cost           decimal     not null,
    cvr_nbr        int         not null,
    imp_nbr        int         not null
);