USE `regbook`;

DROP TABLE IF EXISTS `filter_cities`;

CREATE TABLE `filter_cities` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `identifier` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Идентификатор',
    `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название города (ru)',
    `name_eng` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название города (eng)',
    `country_ru` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название страны (ru)',
    `created_at` timestamp NULL DEFAULT NULL,
    `updated_at` timestamp NULL DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `filter_cities_identifier_unique` (`identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Города';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `filter_countries`;

CREATE TABLE `filter_countries` (
     `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
     `identifier` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Идентификатор',
     `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название страны (ru)',
     `name_eng` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название страны (eng)',
     `created_at` timestamp NULL DEFAULT NULL,
     `updated_at` timestamp NULL DEFAULT NULL,
     PRIMARY KEY (`id`),
     UNIQUE KEY `filter_countries_identifier_unique` (`identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Страны';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `filter_types`;

CREATE TABLE `filter_types` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `identifier` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Идентификатор',
    `code` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Код',
    `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название типа судна (ru)',
    `name_eng` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название типа судна (eng)',
    `created_at` timestamp NULL DEFAULT NULL,
    `updated_at` timestamp NULL DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `filter_types_identifier_unique` (`identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Статистические группы судов';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `filter_classes`;

CREATE TABLE `filter_classes` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `identifier` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Идентификатор',
    `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название ледовой категории (ru)',
    `created_at` timestamp NULL DEFAULT NULL,
    `updated_at` timestamp NULL DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `filter_classes_identifier_unique` (`identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Ледовый класс';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `group_properties`;

CREATE TABLE `group_properties` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название группы характеристик',
    PRIMARY KEY (`id`),
    UNIQUE KEY `group_properties_name_unique` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Группы характеристик';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `properties`;

CREATE TABLE `properties` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Название характеристики',
    `group_id` int(10) unsigned DEFAULT NULL COMMENT 'Группа',
    PRIMARY KEY (`id`),
    UNIQUE KEY `properties_name_unique` (`name`),
    KEY `properties_group_id_foreign` (`group_id`),
    CONSTRAINT `properties_group_id_foreign` FOREIGN KEY (`group_id`) REFERENCES `group_properties` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Характеристики';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `cards`;

CREATE TABLE `cards` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `identifier` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Идентификатор карточки судна',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Карточка судна';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `cards_properties`;

CREATE TABLE `cards_properties` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `card_id` int(10) unsigned DEFAULT NULL COMMENT 'Карточка судна',
    `property_id` int(10) unsigned DEFAULT NULL COMMENT 'Характеристика',
    `property_value` TEXT COLLATE utf8_unicode_ci NOT NULL COMMENT 'Значение характеристики',
    `updated_at` timestamp NULL DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `cards_properties_card_id_foreign` (`card_id`),
    CONSTRAINT `cards_properties_card_id_foreign` FOREIGN KEY (`card_id`) REFERENCES `cards` (`id`) ON DELETE CASCADE,
    KEY `cards_properties_property_id_foreign` (`property_id`),
    CONSTRAINT `cards_properties_property_id_foreign` FOREIGN KEY (`property_id`) REFERENCES `properties` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Характеристики';

/* ------------------------------------------------------------------------------------------------------------------ */
DROP TABLE IF EXISTS `cards_filters`;

CREATE TABLE `cards_filters` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `card_id` int(10) unsigned DEFAULT NULL,

    `filter_city_identifier` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
    `filter_country_identifier` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
    `filter_type_identifier` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
    `filter_class_identifier` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,

    PRIMARY KEY (`id`),

    KEY `cards_filters_card_id_foreign` (`card_id`),
    CONSTRAINT `cards_filters_card_id_foreign` FOREIGN KEY (`card_id`) REFERENCES `cards` (`id`) ON DELETE SET NULL,

    KEY `cards_filters_filter_city_identifier_foreign` (`filter_city_identifier`),
    CONSTRAINT `cards_filters_filter_city_identifier_foreign` FOREIGN KEY (`filter_city_identifier`) REFERENCES `filter_cities` (`identifier`) ON DELETE SET NULL,

    KEY `cards_filters_filter_country_identifier_foreign` (`filter_country_identifier`),
    CONSTRAINT `cards_filters_filter_country_identifier_foreign` FOREIGN KEY (`filter_country_identifier`) REFERENCES `filter_countries` (`identifier`) ON DELETE SET NULL,

    KEY `cards_filters_filter_type_identifier_foreign` (`filter_type_identifier`),
    CONSTRAINT `cards_filters_filter_type_identifier_foreign` FOREIGN KEY (`filter_type_identifier`) REFERENCES `filter_types` (`identifier`) ON DELETE SET NULL,

    KEY `cards_filters_filter_class_identifier_foreign` (`filter_class_identifier`),
    CONSTRAINT `cards_filters_filter_class_identifier_foreign` FOREIGN KEY (`filter_class_identifier`) REFERENCES `filter_classes` (`identifier`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Зависимости карточек с фильтрами';

/* ------------------------------------------------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `card_certificates`;

CREATE TABLE `card_certificates` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `card_id` int(10) unsigned DEFAULT NULL,
    `e_cert` varchar(255) COLLATE utf8_unicode_ci NOT NULL    COMMENT 'E-cert',
    `type` varchar(255) COLLATE utf8_unicode_ci NOT NULL      COMMENT 'Тип',
    `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL      COMMENT 'Название свидетельства',
    `code` varchar(255) COLLATE utf8_unicode_ci NOT NULL      COMMENT 'Код',
    `created_at` timestamp NULL DEFAULT NULL                  COMMENT 'Дата выдачи (с)',
    `closed_at` timestamp NULL DEFAULT NULL                   COMMENT 'Срок действия (по)',
    `new_closed_at` timestamp NULL DEFAULT NULL               COMMENT 'Срок продлен до',
    `state` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT 'Состояние',
    PRIMARY KEY (`id`),
    KEY `card_certificates_card_id_foreign` (`card_id`),
    CONSTRAINT `card_certificates_card_id_foreign` FOREIGN KEY (`card_id`) REFERENCES `cards` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Свидетельства';

/* ------------------------------------------------------------------------------------------------------------------ */
