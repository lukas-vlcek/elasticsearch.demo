package org.elasticsearch.demo;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.testng.FileAssert.fail;

public abstract class AbstractTest {

    Settings settings;
    File tempFolder;
    String tempFolderName;


    @BeforeMethod
    void prepare() throws IOException {

        // path.data location
        tempFolder = new File("tmp");
        tempFolderName = tempFolder.getCanonicalPath();

        if (tempFolder.exists()) {
            FileUtils.deleteDirectory(tempFolder);
        }
        if (!tempFolder.mkdir()) {
            fail("Could not create a temporary folder ["+tempFolderName+"]");
        }

        // Make sure that the index and metadata are not stored on the disk
        // path.data folder is created but we make sure it is removed after test finishes
        settings = settingsBuilder()
                .put("index.store.type", "memory")
                .put("gateway.type", "none")
                .put("path.data", tempFolderName)
                .build();
    }

    @AfterMethod
    void cleanup() throws IOException {
        if (tempFolder.exists()) {
            FileUtils.deleteDirectory(tempFolder);
        }
    }
}
