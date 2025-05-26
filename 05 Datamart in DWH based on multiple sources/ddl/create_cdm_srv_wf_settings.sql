CREATE TABLE cdm.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key UNIQUE (workflow_key)
);