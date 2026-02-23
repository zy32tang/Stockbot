import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class EncodingCheck {
    private static final Set<String> TEXT_EXTENSIONS = Set.of(
            "java",
            "xml",
            "properties",
            "yml",
            "yaml",
            "json",
            "md",
            "txt",
            "csv",
            "sql",
            "html",
            "css",
            "js",
            "sh",
            "bat",
            "cmd",
            "ps1"
    );

    private static final Set<String> TEXT_FILENAMES = Set.of(
            "pom.xml",
            "mvnw",
            "mvnw.cmd",
            "Dockerfile",
            ".editorconfig",
            ".gitattributes",
            ".gitignore",
            ".dockerignore",
            ".classpath",
            ".project"
    );

    private EncodingCheck() {}

    public static void main(String[] args) throws Exception {
        Path root = args.length > 0
                ? Paths.get(args[0]).toAbsolutePath().normalize()
                : Paths.get(".").toAbsolutePath().normalize();

        List<Path> candidates = collectCandidateFiles(root);
        List<String> failures = new ArrayList<>();
        int checked = 0;

        for (Path file : candidates) {
            if (!Files.isRegularFile(file)) {
                continue;
            }
            Path rel = root.relativize(file);
            if (!shouldCheck(rel)) {
                continue;
            }
            checked++;

            byte[] bytes = Files.readAllBytes(file);
            if (hasUtf8Bom(bytes)) {
                failures.add(rel + " : UTF-8 BOM is not allowed");
            }
            if (!isValidUtf8(bytes)) {
                failures.add(rel + " : file is not valid UTF-8");
            }
        }

        if (failures.isEmpty()) {
            System.out.println("Encoding check passed. checked_files=" + checked);
            return;
        }

        System.err.println("Encoding check failed. invalid_files=" + failures.size());
        for (String failure : failures) {
            System.err.println(" - " + failure);
        }
        System.err.println("Fix: re-save listed files as UTF-8 without BOM.");
        System.exit(1);
    }

    private static List<Path> collectCandidateFiles(Path root) throws Exception {
        List<Path> fromGit = collectFromGit(root);
        if (fromGit != null) {
            return fromGit;
        }
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> !isUnderIgnoredDirectory(root, p))
                    .collect(Collectors.toList());
        }
    }

    private static List<Path> collectFromGit(Path root) {
        try {
            Process process = new ProcessBuilder("git", "-C", root.toString(), "ls-files")
                    .redirectErrorStream(true)
                    .start();
            List<Path> out = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        out.add(root.resolve(trimmed).normalize());
                    }
                }
            }
            int exit = process.waitFor();
            if (exit != 0) {
                return null;
            }
            return out;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static boolean shouldCheck(Path relativePath) {
        String rel = relativePath.toString().replace('\\', '/');
        if (rel.startsWith(".git/") || rel.startsWith("target/") || rel.startsWith(".mvn/wrapper/")) {
            return false;
        }

        String filename = relativePath.getFileName().toString();
        if (TEXT_FILENAMES.contains(filename)) {
            return true;
        }

        int dot = filename.lastIndexOf('.');
        if (dot < 0 || dot == filename.length() - 1) {
            return false;
        }
        String ext = filename.substring(dot + 1).toLowerCase(Locale.ROOT);
        return TEXT_EXTENSIONS.contains(ext);
    }

    private static boolean isUnderIgnoredDirectory(Path root, Path path) {
        String rel = root.relativize(path).toString().replace('\\', '/');
        return rel.startsWith(".git/") || rel.startsWith("target/") || rel.startsWith(".mvn/wrapper/");
    }

    private static boolean hasUtf8Bom(byte[] bytes) {
        return bytes.length >= 3
                && (bytes[0] & 0xFF) == 0xEF
                && (bytes[1] & 0xFF) == 0xBB
                && (bytes[2] & 0xFF) == 0xBF;
    }

    private static boolean isValidUtf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            decoder.decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }
}
