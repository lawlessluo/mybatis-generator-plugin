/*
 * Copyright (c) 2017.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.itfsw.mybatis.generator.plugins;

import com.itfsw.mybatis.generator.plugins.utils.*;
import com.itfsw.mybatis.generator.plugins.utils.hook.IUpsertPluginHook;
import org.mybatis.generator.api.IntrospectedColumn;
import org.mybatis.generator.api.IntrospectedTable;
import org.mybatis.generator.api.dom.java.*;
import org.mybatis.generator.api.dom.xml.*;
import org.mybatis.generator.codegen.mybatis3.ListUtilities;
import org.mybatis.generator.codegen.mybatis3.MyBatis3FormattingUtilities;

import java.util.Iterator;
import java.util.List;

/**
 * ---------------------------------------------------------------------------
 * Phoenix-HBase upsert插件
 * ---------------------------------------------------------------------------
 *
 * @author: luolh
 * @time:2017/3/21 10:59
 * ---------------------------------------------------------------------------
 */
public class PhoenixUpsertPlugin extends BasePlugin {
    public static final String METHOD_UPSERT = "upsert";  // 方法名
    public static final String METHOD_UPSERT_SELECTIVE = "upsertSelective";  // 方法名
    public static final String METHOD_BATCH_UPSERT = "batchUpsert";  // 方法名
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(List<String> warnings) {
        // 插件使用前提是数据库为HBase
        if (!PHOENIX_DRIVER.equalsIgnoreCase(this.getContext().getJdbcConnectionConfiguration().getDriverClass())) {
            warnings.add("itfsw:插件" + this.getClass().getTypeName() + "插件使用前提是数据库为HBase！");
            return false;
        }
        return super.validate(warnings);
    }

    /**
     * Java Client Methods 生成
     * 具体执行顺序 http://www.mybatis.org/generator/reference/pluggingIn.html
     *
     * @param interfaze
     * @param topLevelClass
     * @param introspectedTable
     * @return
     */
    @Override
    public boolean clientGenerated(Interface interfaze, TopLevelClass topLevelClass, IntrospectedTable introspectedTable) {
        // ====================================== upsert ======================================
        Method mUpsert = JavaElementGeneratorTools.generateMethod(
                METHOD_UPSERT,
                JavaVisibility.DEFAULT,
                FullyQualifiedJavaType.getIntInstance(),
                new Parameter(JavaElementGeneratorTools.getModelTypeWithoutBLOBs(introspectedTable), "record")
        );
        commentGenerator.addGeneralMethodComment(mUpsert, introspectedTable);
        // interface 增加方法
        FormatTools.addMethodWithBestPosition(interfaze, mUpsert);
        logger.debug("itfsw(存在即更新插件):" + interfaze.getType().getShortName() + "增加upsert方法。");

        // ====================================== upsertSelective ======================================
        // 找出全字段对应的Model
        FullyQualifiedJavaType fullFieldModel = introspectedTable.getRules().calculateAllFieldsClass();
        Method mUpsertSelective = JavaElementGeneratorTools.generateMethod(
                METHOD_UPSERT_SELECTIVE,
                JavaVisibility.DEFAULT,
                FullyQualifiedJavaType.getIntInstance(),
                new Parameter(fullFieldModel, "record")
        );
        commentGenerator.addGeneralMethodComment(mUpsertSelective, introspectedTable);
        // hook
        if (PluginTools.getHook(IUpsertPluginHook.class).clientUpsertSelectiveMethodGenerated(mUpsertSelective, interfaze, introspectedTable)) {
            // interface 增加方法
            FormatTools.addMethodWithBestPosition(interfaze, mUpsertSelective);
            logger.debug("itfsw(存在即更新插件):" + interfaze.getType().getShortName() + "增加upsertSelective方法。");
        }

        // ====================================== batchUpsert ======================================
        FullyQualifiedJavaType listType = FullyQualifiedJavaType.getNewListInstance();
        listType.addTypeArgument(JavaElementGeneratorTools.getModelTypeWithoutBLOBs(introspectedTable));
        Method mBatchUpsert = JavaElementGeneratorTools.generateMethod(
                METHOD_BATCH_UPSERT,
                JavaVisibility.DEFAULT,
                FullyQualifiedJavaType.getIntInstance(),
                new Parameter(listType, "list", "@Param(\"list\")")
        );
        commentGenerator.addGeneralMethodComment(mBatchUpsert, introspectedTable);
        // hook
        if (PluginTools.getHook(IUpsertPluginHook.class).clientUpsertSelectiveMethodGenerated(mBatchUpsert, interfaze, introspectedTable)) {
            // interface 增加方法
            FormatTools.addMethodWithBestPosition(interfaze, mBatchUpsert);
            logger.debug("itfsw(存在即更新插件):" + interfaze.getType().getShortName() + "增加mBatchUpsert方法。");
        }
        return super.clientGenerated(interfaze, topLevelClass, introspectedTable);
    }

    /**
     * SQL Map Methods 生成
     * 具体执行顺序 http://www.mybatis.org/generator/reference/pluggingIn.html
     *
     * @param document
     * @param introspectedTable
     * @return
     */
    @Override
    public boolean sqlMapDocumentGenerated(Document document, IntrospectedTable introspectedTable) {
        this.generateXmlElementWithSelective(document, introspectedTable);
        this.generateXmlElement(document, introspectedTable);
        this.generateBatchXmlElement(document, introspectedTable);
        return super.sqlMapDocumentGenerated(document, introspectedTable);
    }

    /**
     * 当Selective情况
     *
     * @param document
     * @param introspectedTable
     */
    private void generateXmlElementWithSelective(Document document, IntrospectedTable introspectedTable) {
        List<IntrospectedColumn> columns = ListUtilities.removeGeneratedAlwaysColumns(introspectedTable.getAllColumns());

        // ====================================== upsertSelective ======================================
        XmlElement insertEle = new XmlElement("insert");
        insertEle.addAttribute(new Attribute("id", METHOD_UPSERT_SELECTIVE));
        // 添加注释
        commentGenerator.addComment(insertEle);

        // 参数类型
        insertEle.addAttribute(new Attribute("parameterType", introspectedTable.getRules().calculateAllFieldsClass().getFullyQualifiedName()));

        // 使用JDBC的getGenereatedKeys方法获取主键并赋值到keyProperty设置的领域模型属性中。所以只支持MYSQL和SQLServer
        XmlElementGeneratorTools.useGeneratedKeys(insertEle, introspectedTable);

        // insert
        insertEle.addElement(new TextElement("upsert into " + introspectedTable.getFullyQualifiedTableNameAtRuntime()));
        XmlElement insertColumnsEle = XmlElementGeneratorTools.generateKeysSelective(columns);
        insertEle.addElement(insertColumnsEle);
        insertEle.addElement(new TextElement("values"));
        XmlElement insertValuesEle = XmlElementGeneratorTools.generateValuesSelective(columns);
        insertEle.addElement(insertValuesEle);
        // set
        XmlElement setsEle = XmlElementGeneratorTools.generateSetsSelective(columns);
        insertEle.addElement(setsEle);

        // hook
        if (PluginTools.getHook(IUpsertPluginHook.class).sqlMapUpsertSelectiveElementGenerated(insertEle, columns, insertColumnsEle, insertValuesEle, setsEle, introspectedTable)) {
            document.getRootElement().addElement(insertEle);
            logger.debug("itfsw(存在即更新插件):" + introspectedTable.getMyBatis3XmlMapperFileName() + "增加upsertSelective实现方法。");
        }
    }

    /**
     * 生成xml
     *
     * @param document
     * @param introspectedTable
     */
    private void generateXmlElement(Document document, IntrospectedTable introspectedTable) {
        List<IntrospectedColumn> columns = ListUtilities.removeGeneratedAlwaysColumns(introspectedTable.getNonBLOBColumns());
        // ====================================== upsert ======================================
        XmlElement insertEle = new XmlElement("insert");
        insertEle.addAttribute(new Attribute("id", METHOD_UPSERT));
        // 添加注释(!!!必须添加注释，overwrite覆盖生成时，@see XmlFileMergerJaxp.isGeneratedNode会去判断注释中是否存在OLD_ELEMENT_TAGS中的一点，例子：@mbg.generated)
        commentGenerator.addComment(insertEle);

        // 参数类型
        insertEle.addAttribute(new Attribute("parameterType", JavaElementGeneratorTools.getModelTypeWithoutBLOBs(introspectedTable).getFullyQualifiedName()));
        // 使用JDBC的getGenereatedKeys方法获取主键并赋值到keyProperty设置的领域模型属性中。所以只支持MYSQL和SQLServer
        XmlElementGeneratorTools.useGeneratedKeys(insertEle, introspectedTable);

        // insert
        insertEle.addElement(new TextElement("upsert into " + introspectedTable.getFullyQualifiedTableNameAtRuntime()));
        for (Element element : XmlElementGeneratorTools.generateUpsertKeys(columns, null)) {
            insertEle.addElement(element);
        }
        insertEle.addElement(new TextElement("values"));
        for (Element element : XmlElementGeneratorTools.generateUpsertValues(columns, null, true)) {
            insertEle.addElement(element);
        }

        document.getRootElement().addElement(insertEle);
        logger.debug("itfsw(存在即更新插件):" + introspectedTable.getMyBatis3XmlMapperFileName() + "增加" + ("upsert") + "实现方法。");
    }

    /**
     * 批量
     * @param document
     * @param introspectedTable
     */
    private void generateBatchXmlElement(Document document, IntrospectedTable introspectedTable) {
        List<IntrospectedColumn> columns = ListUtilities.removeGeneratedAlwaysColumns(introspectedTable.getNonBLOBColumns());
        XmlElement insertEle = new XmlElement("insert");
        insertEle.addAttribute(new Attribute("id", METHOD_BATCH_UPSERT));
        // 添加注释
        commentGenerator.addComment(insertEle);

        // 参数类型
        insertEle.addAttribute(new Attribute("parameterType", JavaElementGeneratorTools.getModelTypeWithoutBLOBs(introspectedTable).getFullyQualifiedName()));
        // upsert
        insertEle.addElement(new TextElement("upsert into " + introspectedTable.getFullyQualifiedTableNameAtRuntime()));
        for (Element element : XmlElementGeneratorTools.generateKeys(columns, true)) {
            insertEle.addElement(element);
        }

        XmlElement foreachEle = new XmlElement("foreach");
        insertEle.addElement(foreachEle);
        foreachEle.addAttribute(new Attribute("collection", "list"));
        foreachEle.addAttribute(new Attribute("item", "item"));
        foreachEle.addAttribute(new Attribute("separator", " union all "));

        foreachEle.addElement(new TextElement("select "));
        for (Element element : XmlElementGeneratorTools.generateValues(columns, "item.", false)) {
            foreachEle.addElement(element);
        }

        document.getRootElement().addElement(insertEle);
        logger.debug("itfsw(存在即更新插件):" + introspectedTable.getMyBatis3XmlMapperFileName() + "增加" + ("batchUpsert") + "实现方法。");
    }
}