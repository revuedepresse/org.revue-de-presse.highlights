CREATE TABLE weaving_user (usr_id INT AUTO_INCREMENT NOT NULL, usr_twitter_id VARCHAR(255) DEFAULT NULL, usr_twitter_username VARCHAR(255) DEFAULT NULL, usr_avatar INT DEFAULT NULL, usr_full_name VARCHAR(255) DEFAULT NULL, usr_status TINYINT(1) NOT NULL, usr_user_name VARCHAR(255) DEFAULT NULL, usr_username_canonical VARCHAR(255) DEFAULT NULL, usr_email VARCHAR(255) NOT NULL, usr_email_canonical VARCHAR(255) DEFAULT NULL, usr_api_key VARCHAR(255) DEFAULT NULL, protected TINYINT(1) DEFAULT '0' NOT NULL, suspended TINYINT(1) DEFAULT '0' NOT NULL, not_found TINYINT(1) DEFAULT '0' NOT NULL, max_status_id VARCHAR(255) DEFAULT NULL, min_status_id VARCHAR(255) DEFAULT NULL, max_like_id VARCHAR(255) DEFAULT NULL, min_like_id VARCHAR(255) DEFAULT NULL, total_statuses INT DEFAULT 0 NOT NULL, total_likes INT DEFAULT 0 NOT NULL, description LONGTEXT DEFAULT NULL, url LONGTEXT DEFAULT NULL, last_status_publication_date DATETIME DEFAULT NULL, total_subscribees INT DEFAULT 0 NOT NULL, total_subscriptions INT DEFAULT 0 NOT NULL, usr_position_in_hierarchy INT NOT NULL, INDEX membership (usr_id, usr_twitter_id, usr_twitter_username, not_found, protected, suspended, total_subscribees, total_subscriptions), UNIQUE INDEX unique_twitter_id (usr_twitter_id), PRIMARY KEY(usr_id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_user_token (user_id INT NOT NULL, token_id INT NOT NULL, INDEX IDX_44F4C05CA76ED395 (user_id), INDEX IDX_44F4C05C41DEE7B9 (token_id), PRIMARY KEY(user_id, token_id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_status (ust_id INT AUTO_INCREMENT NOT NULL, ust_hash VARCHAR(40) DEFAULT NULL, ust_full_name VARCHAR(32) NOT NULL, ust_name LONGTEXT NOT NULL, ust_text LONGTEXT NOT NULL, ust_avatar VARCHAR(255) NOT NULL, ust_access_token VARCHAR(255) NOT NULL, ust_status_id VARCHAR(255) DEFAULT NULL, ust_api_document LONGTEXT DEFAULT NULL, ust_starred TINYINT(1) DEFAULT '0' NOT NULL, ust_indexed TINYINT(1) DEFAULT '0' NOT NULL, ust_created_at DATETIME NOT NULL, ust_updated_at DATETIME DEFAULT NULL, INDEX screen_name (ust_full_name), INDEX status_id (ust_status_id), INDEX indexed (ust_indexed), INDEX ust_created_at (ust_created_at), UNIQUE INDEX unique_hash (ust_hash), PRIMARY KEY(ust_id)) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENGINE = InnoDB;
CREATE TABLE weaving_status_aggregate (status_id INT NOT NULL, aggregate_id INT NOT NULL, INDEX IDX_53DF6C4D6BF700BD (status_id), INDEX IDX_53DF6C4DD0BBCCBE (aggregate_id), PRIMARY KEY(status_id, aggregate_id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_aggregate (id INT AUTO_INCREMENT NOT NULL, name VARCHAR(255) NOT NULL, screen_name VARCHAR(255) DEFAULT NULL, locked TINYINT(1) NOT NULL, locked_at DATETIME DEFAULT NULL, unlocked_at DATETIME DEFAULT NULL, list_id VARCHAR(255) DEFAULT NULL, created_at DATETIME NOT NULL, INDEX name (name), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_archived_status (ust_id INT AUTO_INCREMENT NOT NULL, ust_hash VARCHAR(40) DEFAULT NULL, ust_full_name VARCHAR(32) NOT NULL, ust_name LONGTEXT NOT NULL, ust_text LONGTEXT NOT NULL, ust_avatar VARCHAR(255) NOT NULL, ust_access_token VARCHAR(255) NOT NULL, ust_status_id VARCHAR(255) DEFAULT NULL, ust_api_document LONGTEXT DEFAULT NULL, ust_starred TINYINT(1) DEFAULT '0' NOT NULL, ust_indexed TINYINT(1) DEFAULT '0' NOT NULL, ust_created_at DATETIME NOT NULL, ust_updated_at DATETIME DEFAULT NULL, INDEX hash (ust_hash), INDEX screen_name (ust_full_name), INDEX status_id (ust_status_id), INDEX ust_created_at (ust_created_at), UNIQUE INDEX unique_hash (ust_hash, ust_access_token, ust_full_name), PRIMARY KEY(ust_id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_archived_status_aggregate (status_id INT NOT NULL, aggregate_id INT NOT NULL, INDEX IDX_6C6940DA6BF700BD (status_id), INDEX IDX_6C6940DAD0BBCCBE (aggregate_id), PRIMARY KEY(status_id, aggregate_id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_access_token (id INT AUTO_INCREMENT NOT NULL, type INT DEFAULT NULL, token VARCHAR(255) NOT NULL, secret VARCHAR(255) DEFAULT NULL, consumer_key VARCHAR(255) DEFAULT NULL, consumer_secret VARCHAR(255) DEFAULT NULL, frozen_until DATETIME DEFAULT NULL, created_at DATETIME NOT NULL, updated_at DATETIME DEFAULT NULL, INDEX IDX_FEA6740F8CDE5729 (type), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_token_type (id INT AUTO_INCREMENT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_json (jsn_id INT AUTO_INCREMENT NOT NULL, jsn_status TINYINT(1) NOT NULL, jsn_type INT NOT NULL, jsn_hash VARCHAR(32) NOT NULL, jsn_value LONGTEXT NOT NULL, jsn_geolocated TINYINT(1) DEFAULT NULL, INDEX jsn_status (jsn_status, jsn_type), UNIQUE INDEX jsn_hash (jsn_hash), PRIMARY KEY(jsn_id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_whisperer (id INT AUTO_INCREMENT NOT NULL, name VARCHAR(255) NOT NULL, whispers INT DEFAULT 0 NOT NULL, previous_whispers INT DEFAULT 0 NOT NULL, expected_whispers INT DEFAULT 0 NOT NULL, updated_at DATETIME DEFAULT NULL, UNIQUE INDEX unique_name (name), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_group (rol_id INT AUTO_INCREMENT NOT NULL, PRIMARY KEY(rol_id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE weaving_role (id INT AUTO_INCREMENT NOT NULL, name VARCHAR(30) NOT NULL, role VARCHAR(20) NOT NULL, UNIQUE INDEX UNIQ_7AAAFFD957698A6A (role), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE timely_status (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', status_id INT DEFAULT NULL, aggregate_id INT DEFAULT NULL, publication_date_time DATETIME NOT NULL, aggregate_name VARCHAR(100) NOT NULL, member_name VARCHAR(100) NOT NULL, time_range INT NOT NULL, UNIQUE INDEX UNIQ_7A5BB7E86BF700BD (status_id), INDEX IDX_7A5BB7E8D0BBCCBE (aggregate_id), INDEX publication_idx (publication_date_time), INDEX status_idx (status_id, publication_date_time, time_range, aggregate_id, aggregate_name), INDEX member_name (member_name), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE member_aggregate_subscription (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', member_id INT DEFAULT NULL, list_name VARCHAR(255) NOT NULL, list_id VARCHAR(255) NOT NULL, document LONGTEXT NOT NULL, INDEX IDX_9150051A7597D3FE (member_id), INDEX member_aggregate_subscription (id, member_id, list_id), UNIQUE INDEX unique_aggregate_subscription (member_id, list_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE search_matching_status (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', status_id INT DEFAULT NULL, saved_search_id CHAR(36) DEFAULT NULL COMMENT '(DC2Type:uuid)', publication_date_time DATETIME NOT NULL, member_name VARCHAR(100) NOT NULL, time_range INT NOT NULL, INDEX IDX_1039C1616BF700BD (status_id), INDEX IDX_1039C16150DC2954 (saved_search_id), INDEX status_idx (status_id, publication_date_time, time_range, member_name), UNIQUE INDEX unique_status (status_id, saved_search_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE saved_search (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', search_query VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, search_id VARCHAR(255) NOT NULL, created_at DATETIME NOT NULL, INDEX saved_search_idx (id, search_query), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE archived_timely_status (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', status_id INT DEFAULT NULL, aggregate_id INT DEFAULT NULL, publication_date_time DATETIME NOT NULL, aggregate_name VARCHAR(100) NOT NULL, member_name VARCHAR(100) NOT NULL, time_range INT NOT NULL, UNIQUE INDEX UNIQ_80FB72246BF700BD (status_id), INDEX IDX_80FB7224D0BBCCBE (aggregate_id), INDEX publication_idx (publication_date_time), INDEX status_idx (status_id, publication_date_time, time_range, aggregate_id, aggregate_name), INDEX member_name (member_name), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE member_subscribee (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', member_id INT DEFAULT NULL, subscribee_id INT DEFAULT NULL, INDEX IDX_C59DC0477597D3FE (member_id), INDEX IDX_C59DC047B5C6DE8B (subscribee_id), UNIQUE INDEX unique_subscribee (member_id, subscribee_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE member_subscription (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', member_id INT DEFAULT NULL, subscription_id INT DEFAULT NULL, INDEX IDX_D675FA5B7597D3FE (member_id), INDEX IDX_D675FA5B9A1887DC (subscription_id), UNIQUE INDEX unique_subscription (member_id, subscription_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE aggregate_subscription (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', member_aggregate_subscription_id CHAR(36) DEFAULT NULL COMMENT '(DC2Type:uuid)', subscription_id INT DEFAULT NULL, INDEX IDX_80FCFB8B61EC46B6 (member_aggregate_subscription_id), INDEX IDX_80FCFB8B9A1887DC (subscription_id), UNIQUE INDEX unique_subscription (member_aggregate_subscription_id, subscription_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE liked_status (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', status_id INT DEFAULT NULL, archived_status_id INT DEFAULT NULL, member_id INT DEFAULT NULL, liked_by INT DEFAULT NULL, aggregate_id INT DEFAULT NULL, is_archived_status TINYINT(1) NOT NULL, aggregate_name VARCHAR(100) NOT NULL, member_name VARCHAR(100) NOT NULL, liked_by_member_name VARCHAR(100) NOT NULL, publication_date_time DATETIME NOT NULL, time_range INT NOT NULL, INDEX IDX_91987976BF700BD (status_id), INDEX IDX_9198797EFB3BB1D (archived_status_id), INDEX IDX_91987977597D3FE (member_id), INDEX IDX_9198797621FAD6B (liked_by), INDEX IDX_9198797D0BBCCBE (aggregate_id), INDEX status_idx (member_name, aggregate_name, aggregate_id, liked_by_member_name, liked_by, is_archived_status, status_id, archived_status_id, publication_date_time, time_range), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE status_not_found (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', status_id INT DEFAULT NULL, archived_status_id INT DEFAULT NULL, UNIQUE INDEX UNIQ_80A51EBF6BF700BD (status_id), UNIQUE INDEX UNIQ_80A51EBFEFB3BB1D (archived_status_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
CREATE TABLE highlight (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', status_id INT DEFAULT NULL, member_id INT DEFAULT NULL, publication_date_time DATETIME NOT NULL, total_retweets INT NOT NULL, total_favorites INT NOT NULL, UNIQUE INDEX UNIQ_C998D8346BF700BD (status_id), INDEX IDX_C998D8347597D3FE (member_id), INDEX highlight_idx (status_id, member_id, publication_date_time, total_retweets, total_favorites), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
ALTER TABLE weaving_user_token ADD CONSTRAINT FK_44F4C05CA76ED395 FOREIGN KEY (user_id) REFERENCES weaving_user (usr_id);
ALTER TABLE weaving_user_token ADD CONSTRAINT FK_44F4C05C41DEE7B9 FOREIGN KEY (token_id) REFERENCES weaving_access_token (id);
ALTER TABLE weaving_status_aggregate ADD CONSTRAINT FK_53DF6C4D6BF700BD FOREIGN KEY (status_id) REFERENCES weaving_status (ust_id);
ALTER TABLE weaving_status_aggregate ADD CONSTRAINT FK_53DF6C4DD0BBCCBE FOREIGN KEY (aggregate_id) REFERENCES weaving_aggregate (id);
ALTER TABLE weaving_archived_status_aggregate ADD CONSTRAINT FK_6C6940DA6BF700BD FOREIGN KEY (status_id) REFERENCES weaving_archived_status (ust_id);
ALTER TABLE weaving_archived_status_aggregate ADD CONSTRAINT FK_6C6940DAD0BBCCBE FOREIGN KEY (aggregate_id) REFERENCES weaving_aggregate (id);
ALTER TABLE weaving_access_token ADD CONSTRAINT FK_FEA6740F8CDE5729 FOREIGN KEY (type) REFERENCES weaving_token_type (id);
ALTER TABLE timely_status ADD CONSTRAINT FK_7A5BB7E86BF700BD FOREIGN KEY (status_id) REFERENCES weaving_status (ust_id);
ALTER TABLE timely_status ADD CONSTRAINT FK_7A5BB7E8D0BBCCBE FOREIGN KEY (aggregate_id) REFERENCES weaving_aggregate (id);
ALTER TABLE member_aggregate_subscription ADD CONSTRAINT FK_9150051A7597D3FE FOREIGN KEY (member_id) REFERENCES weaving_user (usr_id);
ALTER TABLE search_matching_status ADD CONSTRAINT FK_1039C1616BF700BD FOREIGN KEY (status_id) REFERENCES weaving_status (ust_id);
ALTER TABLE search_matching_status ADD CONSTRAINT FK_1039C16150DC2954 FOREIGN KEY (saved_search_id) REFERENCES saved_search (id);
ALTER TABLE archived_timely_status ADD CONSTRAINT FK_80FB72246BF700BD FOREIGN KEY (status_id) REFERENCES weaving_archived_status (ust_id);
ALTER TABLE archived_timely_status ADD CONSTRAINT FK_80FB7224D0BBCCBE FOREIGN KEY (aggregate_id) REFERENCES weaving_aggregate (id);
ALTER TABLE member_subscribee ADD CONSTRAINT FK_C59DC0477597D3FE FOREIGN KEY (member_id) REFERENCES weaving_user (usr_id);
ALTER TABLE member_subscribee ADD CONSTRAINT FK_C59DC047B5C6DE8B FOREIGN KEY (subscribee_id) REFERENCES weaving_user (usr_id);
ALTER TABLE member_subscription ADD CONSTRAINT FK_D675FA5B7597D3FE FOREIGN KEY (member_id) REFERENCES weaving_user (usr_id);
ALTER TABLE member_subscription ADD CONSTRAINT FK_D675FA5B9A1887DC FOREIGN KEY (subscription_id) REFERENCES weaving_user (usr_id);
ALTER TABLE aggregate_subscription ADD CONSTRAINT FK_80FCFB8B61EC46B6 FOREIGN KEY (member_aggregate_subscription_id) REFERENCES member_aggregate_subscription (id);
ALTER TABLE aggregate_subscription ADD CONSTRAINT FK_80FCFB8B9A1887DC FOREIGN KEY (subscription_id) REFERENCES weaving_user (usr_id);
ALTER TABLE liked_status ADD CONSTRAINT FK_91987976BF700BD FOREIGN KEY (status_id) REFERENCES weaving_status (ust_id);
ALTER TABLE liked_status ADD CONSTRAINT FK_9198797EFB3BB1D FOREIGN KEY (archived_status_id) REFERENCES weaving_archived_status (ust_id);
ALTER TABLE liked_status ADD CONSTRAINT FK_91987977597D3FE FOREIGN KEY (member_id) REFERENCES weaving_user (usr_id);
ALTER TABLE liked_status ADD CONSTRAINT FK_9198797621FAD6B FOREIGN KEY (liked_by) REFERENCES weaving_user (usr_id);
ALTER TABLE liked_status ADD CONSTRAINT FK_9198797D0BBCCBE FOREIGN KEY (aggregate_id) REFERENCES weaving_aggregate (id);
ALTER TABLE status_not_found ADD CONSTRAINT FK_80A51EBF6BF700BD FOREIGN KEY (status_id) REFERENCES weaving_status (ust_id);
ALTER TABLE status_not_found ADD CONSTRAINT FK_80A51EBFEFB3BB1D FOREIGN KEY (archived_status_id) REFERENCES weaving_archived_status (ust_id);
ALTER TABLE highlight ADD CONSTRAINT FK_C998D8346BF700BD FOREIGN KEY (status_id) REFERENCES weaving_status (ust_id);
ALTER TABLE highlight ADD CONSTRAINT FK_C998D8347597D3FE FOREIGN KEY (member_id) REFERENCES weaving_user (usr_id);
ALTER TABLE highlight ADD is_retweet TINYINT(1) NOT NULL;
ALTER TABLE highlight ADD retweeted_status_publication_date DATETIME DEFAULT NULL;
ALTER TABLE highlight ADD aggregate_id INT DEFAULT NULL, ADD aggregate_name VARCHAR(100) DEFAULT NULL;
ALTER TABLE highlight ADD CONSTRAINT FK_C998D834D0BBCCBE FOREIGN KEY (aggregate_id) REFERENCES weaving_aggregate (id);
CREATE INDEX IDX_C998D834D0BBCCBE ON highlight (aggregate_id);
CREATE TABLE keyword (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', status_id INT DEFAULT NULL, member_id INT DEFAULT NULL, aggregate_id INT DEFAULT NULL, aggregate_name VARCHAR(100) DEFAULT NULL, keyword VARCHAR(255) NOT NULL, publication_date_time DATETIME NOT NULL, occurrences INT NOT NULL, INDEX IDX_5A93713B6BF700BD (status_id), INDEX IDX_5A93713B7597D3FE (member_id), INDEX IDX_5A93713BD0BBCCBE (aggregate_id), INDEX keyword_idx (keyword, status_id, member_id, publication_date_time), PRIMARY KEY(id)) DEFAULT CHARACTER SET UTF8 COLLATE UTF8_unicode_ci ENGINE = InnoDB;
ALTER TABLE keyword ADD CONSTRAINT FK_5A93713B6BF700BD FOREIGN KEY (status_id) REFERENCES weaving_status (ust_id);
ALTER TABLE keyword ADD CONSTRAINT FK_5A93713B7597D3FE FOREIGN KEY (member_id) REFERENCES weaving_user (usr_id);
ALTER TABLE keyword ADD CONSTRAINT FK_5A93713BD0BBCCBE FOREIGN KEY (aggregate_id) REFERENCES weaving_aggregate (id);
CREATE TABLE publication_frequency (id CHAR(36) NOT NULL COMMENT '(DC2Type:uuid)', member_id INT DEFAULT NULL, per_day_of_week VARCHAR(255) NOT NULL, per_hour_of_day VARCHAR(255) NOT NULL, updated_at DATETIME NOT NULL, UNIQUE INDEX UNIQ_3A3CBE847597D3FE (member_id), INDEX publication_frequency_idx (member_id, updated_at), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB;
ALTER TABLE publication_frequency ADD CONSTRAINT FK_3A3CBE847597D3FE FOREIGN KEY (member_id) REFERENCES weaving_user (usr_id);