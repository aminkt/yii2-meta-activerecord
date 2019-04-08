<?php

/**
 * @author Chaim Leichman, MIPO Technologies Ltd
 */

namespace aminkt\metaActiveRecord;

use Yii;
use yii\base\InvalidArgumentException;
use yii\db\ActiveRecord;
use yii\db\Exception;
use yii\db\Query;
use yii\db\Schema;

/**
 * Class MetaActiveRecord
 *
 * @package aminkt\metaActiveRecord
 *
 * @author  Amin Keshavarz <ak_1596@yahoo.com>
 *
 * @property mixed  $pkName
 * @property string $dbName
 * @property string $metaAttribute
 */
abstract class MetaActiveRecord extends ActiveRecord
{
    /** @var boolean $autoLoadMetaData Whether meta data should be loaded */
    protected $autoLoadMetaData = true;

    /** @var boolean $autoSaveMetaFields Whether meta data should be loaded */
    protected $autoSaveMetaFields = false;

    /** @var boolean $autoDeleteMetaFields Whether meta data should be delete */
    protected $autoDeleteMetaFields = false;

    /** @var mixed $metaData Array of the this record's meta data */
    protected $metaData = [];

    /** @var array $oldMetaData Old value of meta data. */
    protected $oldMetaData = [];

    /** @var array $metaDataUpdateQueue Queue of meta data key-value pairs to update */
    protected $metaDataUpdateQueue = array();

    /**
     * Override __get of yii\db\ActiveRecord
     *
     * @param string $name the property name
     *
     * @return mixed
     */
    public function __get($name)
    {
        if (in_array($name, $this->metaAttributes())) {
            return $this->getMetaAttribute($name);
        } else {
            return parent::__get($name);
        }
    }

    /**
     * Override __get of yii\db\ActiveRecord
     *
     * @param string $name  the property name or the event name
     * @param mixed  $value the property value
     */
    public function __set($name, $value)
    {
        if (in_array($name, $this->metaAttributes())) {
            $this->setMetaAttribute($name, $value);
        } else {
            parent::__set($name, $value);
        }
    }

    /**
     * Set new value to metadata.
     *
     * @param $name
     * @param $value
     *
     * @return void
     *
     * @author Amin Keshavarz <ak_1596@yahoo.com>
     */
    public function setMetaAttribute($name, $value)
    {
        $this->metaData[$name] = $value;
        if ($this->autoSaveMetaFields && !$this->isNewRecord) {
            $this->enqueueMetaUpdate($name, $value);
        }
    }

    /**
     * Return metadata by name.
     *
     * @param $name
     *
     * @return mixed|null
     *
     * @author Amin Keshavarz <ak_1596@yahoo.com>
     */
    public function getMetaAttribute($name)
    {
        if (!in_array($name, static::metaAttributes())) {
            throw new InvalidArgumentException("Attribute name is invalid.");
        }

        if (empty($this->metaData)) {
            $this->loadMetaData();
        }

        if (!key_exists($name, $this->metaData)) {
            return null;
        }

        return $this->metaData[$name];
    }

    abstract function metaAttributes();

    /**
     * Enqueue a meta key-value pair to be saved when the record is saved
     *
     * @param string $name  the property name or the event name
     * @param mixed  $value the property value
     */
    protected function enqueueMetaUpdate($name, $value)
    {
        if (!is_array($this->metaDataUpdateQueue))
            $this->metaDataUpdateQueue = array();

        $this->metaDataUpdateQueue[$name] = $value;
    }

    public function attributes()
    {
        return array_merge(parent::attributes(), static::metaAttributes());
    }

    /**
     * Catch the afterFind event to load the meta data if the
     * $autoLoadMetaData flag is set to true
     *
     */
    public function afterFind()
    {
        if ($this->autoLoadMetaData)
            $this->loadMetaData();

        parent::afterFind();
    }

    /**
     * Load the meta data for this record
     *
     * @return void
     */
    protected function loadMetaData()
    {
        $rows = (new Query)
            ->select('*')
            ->from($this->metaTableName())
            ->where([
                'record_id' => $this->{$this->getPkName()}
            ])
            ->all();

        $this->metaData = $rows;
        $this->oldMetaData = $rows;
    }

    public function save($runValidation = true, $attributeNames = null)
    {
        if ($this->autoSaveMetaFields && !$this->isNewRecord) {
            return parent::save($runValidation, $attributeNames);
        }

        $transaction = Yii::$app->getDb()->beginTransaction();
        try {
            $save = parent::save($runValidation, $attributeNames);
            if ($save) {
                foreach ($this->metaData as $key => $value) {
//                    if (!is_array($value)) {
//                        $value = [$value];
//                    }

                    if (!$this->saveMetaAttribute($key, $value)) {
                        throw new Exception("Can't save meta data.");
                    }
                }
            } else {
                throw new Exception("Can't save parent model.");
            }
            $transaction->commit();
            return $save;
        } catch (Exception $exception) {
            $this->deleteMetaData();
            $transaction->rollBack();
            Yii::error($exception);
        }
        return false;
    }

    public function afterDelete()
    {
        parent::afterDelete();
        if ($this->autoDeleteMetaFields) {
            $this->deleteMetaData();
        }
    }

    /**
     * Delete meta data.
     *
     * @param string|null $name Name of meta key.
     *
     * @return void
     *
     * @author Amin Keshavarz <ak_1596@yahoo.com>
     * @throws \yii\db\Exception
     */
    public function deleteMetaData($name = null)
    {
        if ($this->assertMetaTable()) {
            return;
        }
        $command = static::getDb()
            ->createCommand();

        if ($name === null) {
            $command
                ->delete($this->metaTableName(), [
                'record_id' => $this->{$this->getPkName()}
            ]);
        } else {
            $command
                ->delete($this->metaTableName(), [
                    'record_id' => $this->{$this->getPkName()},
                    'meta_key' => $name
                ]);
        }

        $command->execute();
    }

    /**
     * Catch the afterSave event to save all of the queued meta data
     *
     */
    public function afterSave($insert, $changedAttributes)
    {
        $queue = $this->metaDataUpdateQueue;

        if (is_array($queue) && count($queue)) {
            foreach ($queue as $name => $value)
                $this->saveMetaAttribute($name, $value);

            $this->metaDataUpdateQueue = array();
        }

        parent::afterSave($insert, $changedAttributes);
    }

    /**
     * Load the value of the named meta attribute
     *
     * @param string $name Property name
     *
     * @return mixed Property value
     * @throws \yii\db\Exception
     */
    public function loadMetaAttribute($name)
    {
        if (!$this->assertMetaTable())
            return null;

        try {
            $row = (new Query)
                ->select('meta_value')
                ->from($this->metaTableName())
                ->where([
                    'record_id' => $this->{$this->getPkName()},
                    'meta_key' => $name
                ])
                ->limit(1)
                ->one();
        } catch (\Exception $exception) {
            $this->createMetaTable();
            return $this->getMetaAttribute($name);
        }

        return is_array($row) ? $row['meta_value'] : null;
    }

    /**
     * @param boolean $autoCreate Create the table if it does not exist
     *
     * @return boolean If table exists
     * @throws \yii\db\Exception
     */
    protected function assertMetaTable($autoCreate = false)
    {
//        $row = (new Query)
//            ->select('*')
//            ->from('information_schema.tables')
//            ->where([
//                'table_schema' => $this->getDbName(),
//                'table_name' => $this->metaTableName()
//            ])
//            ->limit(1)
//            ->all();

        $tableSchema = static::getDb()->schema->getTableSchema($this->metaTableName());


        if ($tableSchema === null) {
            if ($autoCreate) {
                $this->createMetaTable();
                return true;
            } else
                return false;
        } else
            return true;
    }

    /**
     * @link https://github.com/yiisoft/yii2/issues/6533
     * @return string
     */
    public function getDbName()
    {
        $db = static::getDb();
        $dsn = $db->dsn;
        $name = 'dbname';

        if (preg_match('/' . $name . '=([^;]*)/', $dsn, $match)) {
            return $match[1];
        } else {
            return null;
        }
    }

    /**
     * Return the name of the meta table associated with this model
     *
     * @return string
     */
    public function metaTableName()
    {
        $tblName = static::tableName();

        // Add _meta prefix to parent table name
        $tblName = str_replace('}}', '_meta}}', $tblName);

        return $tblName;
    }

    /**
     * Create table if not exist.
     *
     * @return int
     *
     * @author Amin Keshavarz <ak_1596@yahoo.com>
     * @throws \yii\db\Exception
     */
    protected function createMetaTable()
    {
        $db = Yii::$app->db;
        $tbl = $this->metaTableName();

        $ret = $db
            ->createCommand()
            ->createTable($tbl, [
                'id' => Schema::TYPE_BIGPK,
                'record_id' => Schema::TYPE_BIGINT . ' NOT NULL default \'0\'',
                'meta_key' => Schema::TYPE_STRING . ' default NULL',
                'meta_value' => Schema::TYPE_JSON,
            ], 'ENGINE=MyISAM  DEFAULT CHARSET=utf8')
            ->execute();

        if ($ret) {
            $db
                ->createCommand()
                ->createIndex('UNIQUE_META_RECORD', $tbl, ['record_id', 'meta_key'], true)
                ->execute();
        }

        return $ret;
    }

    protected function getPkName()
    {
        $pk = $this->primaryKey();
        $pk = $pk[0];

        return $pk;
    }

    /**
     * save the value of the named meta attribute
     *
     * @param string $name  the property name or the event name
     * @param mixed  $value the property value
     *
     * @return int
     * @throws \yii\db\Exception
     */
    protected function saveMetaAttribute($name, $value)
    {
        // Assert that the meta table exists,
        // and create it if it does not
        $this->assertMetaTable(true);

        $db = Yii::$app->db;
        $tbl = $this->metaTableName();

        $pk = $this->getPkName();

        // Check if we need to create a new record or update an existing record
        if (empty($this->oldMetaData[$name])) {
            $ret = $db
                ->createCommand()
                ->insert($tbl, [
                    'record_id' => $this->{$pk},
                    'meta_key' => $name,
                    'meta_value' => is_scalar($value) ? $value : serialize($value)
                ])
                ->execute();
        } else {
            $ret = $db
                ->createCommand()
                ->update($tbl, [
                    'meta_value' => is_scalar($value) ? $value : serialize($value)
                ], "record_id = '{$this->$pk}' AND meta_key = '{$name}'")
                ->execute();
        }

        // If update succeeded, save the new value right away
        if ($ret)
            $this->metaData[$name] = $value;

        return $ret;
    }
}
