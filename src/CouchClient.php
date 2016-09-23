<?php

namespace Bluora\LaravelCouchClient;

use Doctrine\CouchDB\CouchDBClient;

class CouchClient extends CouchDBClient
{

    /**
     * CouchDB Client.
     *
     * @var \Doctrine\CouchDB\CouchDBClient
     */
    private $couch_db_client;

    /**
     * Database to use.
     *
     * @var string
     */
    protected $database = '';

    /**
     * Configuration source.
     *
     * @var string
     */
    protected $config_name = '';

    /**
     * Template defaults for model.
     *
     * @var array
     */
    protected $template = [];

    /**
     * Date fields and formats.
     *
     * @var array
     */
    protected $date_fields = [];

    /**
     * Value fields and formats.
     * @var array
     */
    protected $value_fields = [];

    /**
     * Create an instance of the client.
     *
     * @return CouchClient
     */
    public function connect()
    {
        $config = config('config.database.couchdb.'.static::$config_name);
        $config['dbname'] = static::$database;
        $this->couch_db_client = $this->create($config);
        return $this;
    }

    /**
     * Update the couch db document.
     *
     * @return CouchClient
     */
    public function commitDocument()
    {
        if (is_null($couch_db_client)) {
            $this->connect();
        }

        $doc = $this->couch_db_client->findDocument($this->id);
        if (is_null($c)) {
            $this->data['id'] = $this->id;
            $this->couch_db_client->postDocument($this->data);
            return $this;
        }

        $this->data['rev'] = $doc['rev'];
        if ($doc !== $this->data) {
            $this->couch_db_client->putDocument($this->data, $this->id, $doc['rev']);
        }
        return $this;
    }

    /**
     * Process dates against the model.
     *
     * @param  Model $model
     *
     * @return void
     */
    public function dates($model)
    {
        // Date value allocation
        foreach ($this->date_fields as $field_name => $date_format) {
            if (!is_null($model->$field_name)) {
                $this->data[$field_name] = $model->$field_name->get($date_format);
            }
        }
    }

    /**
     * Process values against the model.
     *
     * @param  Model $model
     *
     * @return void
     */
    public function values($model)
    {
        // Date value allocation
        foreach ($this->value_fields as $field_name => $date_format) {
            if (!is_null($model->$field_name)) {
                $this->data[$field_name] = $model->$field_name->get($date_format);
            }
        }
    }
}
