package ru.mail.polis.impl.simple;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final File dir;

    public KVDaoImpl(@NotNull File dir) {
        this.dir = dir;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        File file = getFile(key);
        if (!file.exists()) throw new NoSuchElementException();
        return Files.readAllBytes(file.toPath());

    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        Files.write(getFile(key).toPath(), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        File file = getFile(key);
        if (file.exists()) Files.delete(file.toPath());
    }

    @Override
    public void close() throws IOException {

    }

    private File getFile(byte[] key) throws CharacterCodingException {
        CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        return new File(dir, Arrays.toString(key));
    }
}
