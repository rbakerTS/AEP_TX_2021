import arcpy
import os
import shutil

arcpy.env.workspace = "test.gdb"

def main():
    root = '.'
    gdb_name = 'test'
    gdb_path = os.path.join(root, f'{gdb_name}.gdb')
    arcpy.management.CreateFileGDB(root, gdb_name)

    quit()

    # Feature Class ************************************************************************************************

    point_feature_name = 'test'
    point_feature_path = os.path.join(gdb_path, point_feature_name)

    arcpy.CreateFeatureclass_management(gdb_path, point_feature_name, 'POINT', )

    arcpy.management.AddField(point_feature_path, 'name', 'TEXT')
    arcpy.management.AddField(point_feature_path, 'object_id', 'LONG')

    cursor = arcpy.da.InsertCursor(point_feature_path, ['SHAPE@XY', 'name', 'object_id'])

    point = arcpy.PointGeometry(arcpy.Point(0, 0), arcpy.SpatialReference(4326))

    with cursor as c:
        c.insertRow((point, 'keagan2', 1))

    # Relationship Table ************************************************************************************************

    test_table_name = 'test_relationship'
    test_table_path = os.path.join(gdb_path, test_table_name)
    arcpy.management.CreateTable(gdb_path, test_table_name)

    arcpy.management.AddField(test_table_path, 'color', 'TEXT')
    arcpy.management.AddField(test_table_path, 'point_object_id', 'LONG')
    arcpy.management.AddField(test_table_path, 'image_id', 'LONG')

    table_cursor = arcpy.da.InsertCursor(test_table_path, ['color', 'point_object_id','image_id'])
    with table_cursor as tc:
        tc.insertRow(('blue', 1,1))

    arcpy.management.CreateRelationshipClass(point_feature_path, test_table_path, 'test_relate', 'SIMPLE',
                                             'attributes from test table', 'attributes from point feature', 'NONE',
                                             'ONE_TO_ONE', 'NONE','object_id','point_object_id')

    # Attachments ************************************************************************************************

    arcpy.EnableAttachments_management(test_table_path)

    in_match_join_field = in_join_field = 'image_id'
    in_match_table = 'images.csv'
    in_match_path_field = 'path'
    in_working_folder = './images'

    arcpy.management.AddAttachments(
        test_table_path,
        in_join_field,
        in_match_table,
        in_match_join_field,
        in_match_path_field,
        in_working_folder)



if __name__ == '__main__':
    main()
