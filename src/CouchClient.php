<?php

namespace Bluora\LaravelCouchClient;

use Doctrine\CouchDB\CouchDBClient;
use Doctrine\CouchDB\Attachment;
use Diff\Differ\MapDiffer;
use GuzzleHttp\Client as Guzzle;

class CouchClient
{
    /**
     * CouchDB Client.
     *
     * @var \Doctrine\CouchDB\CouchDBClient
     */
    private $couch_db_client;

    /**
     * Config.
     *
     * @var array
     */
    protected $config = [];

    /**
     * Base URL.
     *
     * @var string
     */
    protected $base_url = '';

    /**
     * Database to use.
     *
     * @var string
     */
    protected $database = '';

    /**
     * The configuration that the connection to CouchDB should use.
     *
     * @var string
     */
    protected $config_name = 'default';

    /**
     * Related record id.
     *
     * @var string
     */
    protected $related_record_id = 0;

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
     * Attachments.
     * @var array
     */
    protected $attachments = [];

    /**
     * Last connection had an error
     * @var boolean
     */
    protected $had_error = false;

    /**
     * Error message store
     * @var array
     */
    protected $last_error = [];

    /**
     * Construct the class. If $id is set, we push automatically.
     *
     * @param  integer $id
     *
     * @return Books
     */
    public function __construct($id = null)
    {
        if (!empty($id)) {
            $this->push($id);
        }
        return $this;
    }

    /**
     * Create an instance of the client.
     *
     * @return CouchClient
     */
    private function connect()
    {
        $this->config = config('database.couchdb.'.$this->config_name);
        $this->config['dbname'] = $this->database;
        $this->couch_db_client = CouchDBClient::create($this->config);
        $this->base_url = sprintf('%s://%s:%s/%s', $this->config['scheme'], $this->config['host'], $this->config['port'], $this->database);
        return $this;
    }

    /**
     * Set a related record id.
     *
     * @param integer $related_record_id
     *
     * @return void
     */
    public function setRelatedRecordId($related_record_id)
    {
        $this->related_record_id = $related_record_id;
    }

    /**
     * Make the call to the parent wrapper.
     *
     * @return mixed
     */
    private function call()
    {
        // Reset error related variables.
        $this->had_error = false;
        $this->last_error = [];

        // No connection has been made, so let's connect
        if (is_null($this->couch_db_client)) {
            $this->connect();
        }

        // Arguments provided.
        $arguments = func_get_args();

        // First argument is the method we need to call.
        $method_name = array_shift($arguments);

        // Method exists, so continue.
        if (method_exists($this->couch_db_client, $method_name)) {
            // Make the parent wrapper call
            $result = $this->couch_db_client->$method_name(...$arguments);
            
            // Result is an array, so return.
            if (is_array($result)) {
                return $result;
            }

            // Result status says it was successful, return the body.
            if ($result->status == 200) {
                return $result->body;
            }

            // The error message that was returned means we don't have a database.
            // Let's create it and return null as this record won't exist.
            if ($result->body['error'] === 'not_found'
                && in_array($result->body['reason'], ['Database does not exist.', 'no_db_file'])) {
                $this->couch_db_client->createDatabase($this->database);

                return null;
            }

            // If we're requesting documents, and the error is not found/missing
            // we'll return null as the record doesn't exist.
            elseif (($method_name == 'findDocument' || $method_name == 'findDocuments')
                && $result->body['error'] === 'not_found'
                && in_array($result->body['reason'], ['missing'])) {

                return null;
            }

            // Change method as the document should exist.
            elseif ($method_name == 'findDocument'
                && $result->body['error'] === 'not_found'
                && in_array($result->body['reason'], ['deleted'])) {

                return null;
            }

            // Standard error occured that wasn't covered above.
            $this->had_error = true;
            $this->last_error = $result->body;
            $this->last_error['status'] = $result->status;

            return false;
        }

        // The method to the parent wrapper didn't exist.
        $this->had_error = true;
        $this->last_error = [
            'status' => 500,
            'error' => 'method_not_exist',
            'reason' => 'Method called does not exist.s'
        ];

        return false;
    }

    /**
     * Process dates against the model.
     *
     * @param  Model $model
     *
     * @return CouchClient
     */
    public function dates($model)
    {
        // Date value allocation
        foreach ($this->date_fields as $field_name => $date_format) {
            if (!is_null($model->$field_name)) {
                $this->data[$field_name] = $model->$field_name->format($date_format);
            }
        }

        return $this;
    }

    /**
     * Process values against the model.
     *
     * @param  Model $model
     *
     * @return CouchClient
     */
    public function values($model)
    {
        // Date value allocation
        foreach ($this->value_fields as $field_name => $date_format) {
            if (!is_null($model->$field_name)) {
                $this->data[$field_name] = $model->$field_name->get($date_format);
            }
        }

        return $this;
    }

    /**
     * Set the document id.
     *
     * @param  integer $id
     *
     * @return boolean
     */
    public function setId($id)
    {
        $this->id = (string) $id;
        $this->data = $this->template;
        $this->had_error = false;
        $this->last_error = [];
        $this->attachments = [];

        return $this;
    }

    /**
     * Check, add new or update existing document.
     *
     * @return boolean
     */
    public function addOrUpdateDocument()
    {
        if (false === ($current_document = $this->call('findDocument', $this->id))) {
            return false;
        }

        if (is_null($current_document)) {
            $this->data['_id'] = $this->id;
            list($_id, $_rev) = $this->call('postDocument', $this->data);
            $this->data['_rev'] = $_rev;
            $this->uploadAttachments($this->data, $this->attachments);
            return true;
        }

        $this->data['_id'] = $current_document['_id'];
        $this->data['_rev'] = $current_document['_rev'];
        if (isset($current_document['_attachments'])) {
            $this->data['_attachments'] = $current_document['_attachments'];
        }

        $difference = (new MapDiffer())->doDiff($current_document, $this->data);

        if (count($difference)) {
            $this->call('putDocument', $this->data, $this->id, $current_document['_rev']);
        }

        $attachments = $this->compareAttachments($current_document['_id']);
        if (count($attachments)) {
            $this->uploadAttachments($current_document, $attachments);
        }

        return true;
    }

    private function compareAttachments($document_id)
    {
        if (count($this->attachments) == 0) {
            return [];
        }

        $response = (new Guzzle())->request('GET', sprintf('%s/%s', $this->base_url, $document_id));
        $current_document = json_decode($response->getBody()->getContents(), true);

        $current_attachments = isset($current_document['_attachments']) ? $current_document['_attachments'] : [];

        $attachments = [];

        foreach ($this->attachments as $file_name => $details) {

            // File is not present in current attachments
            if (!isset($current_attachments[$file_name])) {
                $attachments[$file_name] = $details;
                continue;
            }

            // Compare the base_64 md5 digests
            $current_digest = substr($current_attachments[$file_name]['digest'], 4);
            $new_digest = base64_encode(md5(file_get_contents($details['file_path']), 1));

            if ($current_digest != $new_digest) {
                $attachments[$file_name] = $details;
                continue;
            }
        }

        $this->attachments = [];
        return $attachments;
    }

    /**
     * Add an attachment to document.
     *
     * @param string $file_name
     * @param string $content_type
     * @param string $source_file_path
     *
     * @return void
     */
    public function addAttachment($file_name, $content_type, $file_path)
    {
        $this->attachments[$file_name] = [
            'content_type'  => $content_type,
            'file_path'     => $file_path,
        ];
    }

    /**
     * Push attachments.
     *
     * @return mixed
     */
    private function uploadAttachments($current_document, $attachments)
    {
        $client = new Guzzle();

        foreach ($attachments as $filename => $details) {
            if (!file_exists($details['file_path'])) {
                continue;
            }
            try {
                $url = sprintf('%s/%s/%s', $this->base_url, $current_document['_id'], $filename);
                $response = $client->request('PUT', $url, [
                    'query' => ['rev' => $current_document['_rev']],
                    'headers' => ['Content-Type' => $details['content_type']],
                    'body' => fopen($details['file_path'], 'r'),
                ], ['curl' => [CURLOPT_BINARYTRANSFER => '1']]);
            } catch (\Exception $exception) {}
        }
    }

    /**
     * Check if an error occured in the last request.
     *
     * @return boolean
     */
    public function hadError()
    {
        return $this->had_error;
    }

    /**
     * Return the error message information.
     *
     * @return boolean
     */
    public function getError()
    {
        return $this->last_error;
    }

    /**
     * Return the source model.
     *
     * @return boolean
     */
    public function getSourceModel()
    {
        return $this->source_model;
    }
}
