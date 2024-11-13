
    CREATE TABLE IF NOT EXISTS default.github_action_events_local
    on cluster replicated(`search_key_updated_at` Int64,
`search_key_event_type` String,
`search_key_gh_archive_year` String,
`search_key_gh_archive_month` String,
`search_key_gh_archive_hour` String,
`search_key_gh_archive_day` String,
`search_key_owner` String,
`search_key_repo` String,
`search_key_id` String,
`id` String,
`type` String,
`actor__id` Int64,
`actor__login` String,
`actor__display_login` String,
`repo__id` Int64,
`repo__name` String,
`payload__ref` String,
`payload__ref_type` String,
`payload__master_branch` String,
`payload__description` String,
`payload__pusher_type` String,
`public` Int64,
`created_at` DateTime64(3),
`org__id` Int64,
`org__login` String,
`payload__forkee__id` Int64,
`payload__forkee__node_id` String,
`payload__forkee__name` String,
`payload__forkee__full_name` String,
`payload__forkee__private` Int64,
`payload__forkee__owner__login` String,
`payload__forkee__owner__id` Int64,
`payload__forkee__owner__node_id` String,
`payload__forkee__owner__type` String,
`payload__forkee__description` String,
`payload__forkee__fork` Int64,
`payload__forkee__created_at` DateTime64(3),
`payload__forkee__updated_at` DateTime64(3),
`payload__forkee__pushed_at` DateTime64(3),
`payload__forkee__stargazers_count` Int64,
`payload__forkee__watchers_count` Int64,
`payload__forkee__language` String,
`payload__forkee__forks_count` Int64,
`payload__forkee__archived` Int64,
`payload__forkee__disabled` Int64,
`payload__forkee__license__key` String,
`payload__forkee__license__name` String,
`payload__forkee__license__spdx_id` String,
`payload__forkee__license__node_id` String,
`payload__forkee__topics` Array(String),
`payload__forkee__forks` Int64,
`payload__forkee__open_issues` Int64,
`payload__forkee__watchers` Int64,
`payload__forkee__default_branch` String,
`payload__forkee__public` Int64,
`payload__pages` Nested(page_name String,title String,summary String,action String,sha String),
`payload__action` String,
`payload__issue__id` Int64,
`payload__issue__node_id` String,
`payload__issue__number` Int64,
`payload__issue__title` String,
`payload__issue__user__login` String,
`payload__issue__user__id` Int64,
`payload__issue__user__node_id` String,
`payload__issue__user__type` String,
`payload__issue__assignee__login` String,
`payload__issue__assignee__id` Int64,
`payload__issue__assignee__node_id` String,
`payload__issue__assignee__type` String,
`payload__issue__assignees` Nested(login String,id Int64,node_id String,type String),
`payload__issue__state` String,
`payload__issue__comments` Int64,
`payload__issue__created_at` DateTime64(3),
`payload__issue__updated_at` DateTime64(3),
`payload__issue__closed_at` DateTime64(3),
`payload__issue__body` String,
`payload__comment__id` Int64,
`payload__comment__node_id` String,
`payload__comment__user__login` String,
`payload__comment__user__id` Int64,
`payload__comment__user__node_id` String,
`payload__comment__user__type` String,
`payload__comment__created_at` DateTime64(3),
`payload__comment__updated_at` DateTime64(3),
`payload__comment__body` String,
`payload__member__login` String,
`payload__member__id` Int64,
`payload__member__node_id` String,
`payload__member__type` String,
`payload__number` Int64,
`payload__pull_request__id` Int64,
`payload__pull_request__node_id` String,
`payload__pull_request__number` Int64,
`payload__pull_request__state` String,
`payload__pull_request__title` String,
`payload__pull_request__user__login` String,
`payload__pull_request__user__id` Int64,
`payload__pull_request__user__node_id` String,
`payload__pull_request__user__type` String,
`payload__pull_request__body` String,
`payload__pull_request__created_at` DateTime64(3),
`payload__pull_request__updated_at` DateTime64(3),
`payload__pull_request__closed_at` DateTime64(3),
`payload__pull_request__merged_at` DateTime64(3),
`payload__pull_request__assignee__login` String,
`payload__pull_request__assignee__id` Int64,
`payload__pull_request__assignee__node_id` String,
`payload__pull_request__assignee__type` String,
`payload__pull_request__assignees` Nested(login String,id Int64,node_id String,type String),
`payload__pull_request__requested_reviewers` Nested(login String,id Int64,node_id String,type String),
`payload__pull_request__head__label` String,
`payload__pull_request__head__ref` String,
`payload__pull_request__head__sha` String,
`payload__pull_request__head__user__login` String,
`payload__pull_request__head__user__id` Int64,
`payload__pull_request__head__user__node_id` String,
`payload__pull_request__head__user__type` String,
`payload__pull_request__head__repo__id` Int64,
`payload__pull_request__head__repo__node_id` String,
`payload__pull_request__head__repo__name` String,
`payload__pull_request__head__repo__full_name` String,
`payload__pull_request__head__repo__private` Int64,
`payload__pull_request__head__repo__owner__login` String,
`payload__pull_request__head__repo__owner__id` Int64,
`payload__pull_request__head__repo__owner__node_id` String,
`payload__pull_request__head__repo__owner__type` String,
`payload__pull_request__head__repo__description` String,
`payload__pull_request__head__repo__fork` Int64,
`payload__pull_request__head__repo__created_at` DateTime64(3),
`payload__pull_request__head__repo__updated_at` DateTime64(3),
`payload__pull_request__head__repo__pushed_at` DateTime64(3),
`payload__pull_request__head__repo__stargazers_count` Int64,
`payload__pull_request__head__repo__watchers_count` Int64,
`payload__pull_request__head__repo__language` String,
`payload__pull_request__head__repo__forks_count` Int64,
`payload__pull_request__head__repo__archived` Int64,
`payload__pull_request__head__repo__disabled` Int64,
`payload__pull_request__head__repo__open_issues_count` Int64,
`payload__pull_request__head__repo__license__key` String,
`payload__pull_request__head__repo__license__name` String,
`payload__pull_request__head__repo__license__spdx_id` String,
`payload__pull_request__head__repo__license__node_id` String,
`payload__pull_request__head__repo__topics` Array(String),
`payload__pull_request__head__repo__forks` Int64,
`payload__pull_request__head__repo__open_issues` Int64,
`payload__pull_request__head__repo__watchers` Int64,
`payload__pull_request__head__repo__default_branch` String,
`payload__pull_request__base__label` String,
`payload__pull_request__base__ref` String,
`payload__pull_request__base__sha` String,
`payload__pull_request__base__user__login` String,
`payload__pull_request__base__user__id` Int64,
`payload__pull_request__base__user__node_id` String,
`payload__pull_request__base__user__type` String,
`payload__pull_request__base__repo__id` Int64,
`payload__pull_request__base__repo__node_id` String,
`payload__pull_request__base__repo__name` String,
`payload__pull_request__base__repo__full_name` String,
`payload__pull_request__base__repo__private` Int64,
`payload__pull_request__base__repo__owner__login` String,
`payload__pull_request__base__repo__owner__id` Int64,
`payload__pull_request__base__repo__owner__node_id` String,
`payload__pull_request__base__repo__owner__type` String,
`payload__pull_request__base__repo__description` String,
`payload__pull_request__base__repo__fork` Int64,
`payload__pull_request__base__repo__created_at` DateTime64(3),
`payload__pull_request__base__repo__updated_at` DateTime64(3),
`payload__pull_request__base__repo__pushed_at` DateTime64(3),
`payload__pull_request__base__repo__stargazers_count` Int64,
`payload__pull_request__base__repo__watchers_count` Int64,
`payload__pull_request__base__repo__language` String,
`payload__pull_request__base__repo__forks_count` Int64,
`payload__pull_request__base__repo__archived` Int64,
`payload__pull_request__base__repo__disabled` Int64,
`payload__pull_request__base__repo__open_issues_count` Int64,
`payload__pull_request__base__repo__license__key` String,
`payload__pull_request__base__repo__license__name` String,
`payload__pull_request__base__repo__license__spdx_id` String,
`payload__pull_request__base__repo__license__node_id` String,
`payload__pull_request__base__repo__topics` Array(String),
`payload__pull_request__base__repo__forks` Int64,
`payload__pull_request__base__repo__open_issues` Int64,
`payload__pull_request__base__repo__watchers` Int64,
`payload__pull_request__base__repo__default_branch` String,
`payload__pull_request__author_association` String,
`payload__pull_request__merged` Int64,
`payload__pull_request__merged_by__login` String,
`payload__pull_request__merged_by__id` Int64,
`payload__pull_request__merged_by__node_id` String,
`payload__pull_request__merged_by__type` String,
`payload__pull_request__comments` Int64,
`payload__pull_request__review_comments` Int64,
`payload__pull_request__commits` Int64,
`payload__pull_request__additions` Int64,
`payload__pull_request__deletions` Int64,
`payload__pull_request__changed_files` Int64,
`payload__review__id` Int64,
`payload__review__node_id` String,
`payload__review__user__login` String,
`payload__review__user__id` Int64,
`payload__review__user__node_id` String,
`payload__review__user__type` String,
`payload__review__body` String,
`payload__review__commit_id` String,
`payload__review__submitted_at` DateTime64(3),
`payload__review__state` String,
`payload__review__author_association` String,
`payload__pull_request__merge_commit_sha` String,
`payload__pull_request__base__repo__size` Int64,
`payload__public` Int64,
`payload__created_at` DateTime64(3),
`payload__org__id` Int64,
`payload__org__login` String,
`payload__comment__pull_request_review_id` Int64,
`payload__comment__diff_hunk` String,
`payload__comment__path` String,
`payload__comment__commit_id` String,
`payload__comment__original_commit_id` String,
`payload__comment__author_association` String,
`payload__comment__line` Int64,
`payload__comment__original_line` Int64,
`payload__comment__side` String,
`payload__comment__original_position` Int64,
`payload__comment__position` Int64,
`payload__comment__subject_type` String,
`payload__repository_id` Int64,
`payload__push_id` Int64,
`payload__size` Int64,
`payload__distinct_size` Int64,
`payload__head` String,
`payload__before` String,
`payload__commits` Nested(sha String,author__email String,author__name String,message String),
`payload__release__id` Int64,
`payload__release__author__login` String,
`payload__release__author__id` Int64,
`payload__release__author__node_id` String,
`payload__release__author__type` String,
`payload__release__node_id` String,
`payload__release__tag_name` String,
`payload__release__target_commitish` String,
`payload__release__name` String,
`payload__release__created_at` DateTime64(3),
`payload__release__published_at` DateTime64(3),
`payload__release__body` String) Engine=ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/github_action_events','{replica}',search_key_updated_at)
     PARTITION BY ('search_key_gh_archive_year', 'search_key_event_type') ORDER BY (id)
    CREATE TABLE IF NOT EXISTS default.github_action_events
    on cluster replicated as default.github_action_events_local
    Engine= Distributed(replicated, default,github_action_events_local,halfMD5(id))
    