; ========================================================
; Sincrona Universal Fiscal Bridge - Enterprise Installer
; Inno Setup Script
; ========================================================

#define MyAppName "Sincrona Universal Fiscal Bridge"
#define MyAppVersion "1.0.1"
#define MyAppPublisher "Sincrona Auto"
#define MyAppURL "https://sincrona.ro"
#define MyAppExeName "SincronaBridge.exe"
#define MyServiceName "SincronaBridge"

[Setup]
AppId={{C789A5F1-57DE-4A6F-9C12-87A4F825D430}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName=C:\SincronaBridge
DefaultGroupName=Sincrona
DisableProgramGroupPage=yes
OutputBaseFilename=SincronaSetup
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
PrivilegesRequiredOverridesAllowed=commandline
ArchitecturesInstallIn64BitMode=x64compatible
OutputDir=installer_output

[Languages]
Name: "en"; MessagesFile: "compiler:Default.isl"

[Dirs]
Name: "{app}\logs"; Permissions: everyone-full

[Files]
; Copiaza intreg continutul distributiei standalone Nuitka
Source: "dist\erpnet_fp_bridge.dist\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "nssm.exe"; DestDir: "{app}"; Flags: ignoreversion

[Code]
// Whitelist automat in Windows Defender inainte de scrierea fisierelor
procedure CurStepChanged(CurStep: TSetupStep);
var
  ResultCode: Integer;
  AppPath: String;
begin
  if CurStep = ssInstall then
  begin
    AppPath := ExpandConstant('{app}');
    // Executie PowerShell ascunsa pentru Add-MpPreference
    Exec('powershell.exe',
      '-ExecutionPolicy Bypass -WindowStyle Hidden -NoProfile -Command "Add-MpPreference -ExclusionPath ''' + AppPath + '''"',
      '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end;
end;

[Run]
; 1. Oprire serviciu daca exista deja dintr-o instalare precedenta
Filename: "{app}\nssm.exe"; Parameters: "stop {#MyServiceName}"; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "remove {#MyServiceName} confirm"; Flags: runhidden waituntilterminated

; 2. Inregistrare Serviciu Windows prin NSSM
Filename: "{app}\nssm.exe"; Parameters: "install {#MyServiceName} ""{app}\{#MyAppExeName}"""; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} AppDirectory ""{app}"""; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} AppStdout ""{app}\logs\service_stdout.log"""; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} AppStderr ""{app}\logs\service_stderr.log"""; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} AppRotateFiles 1"; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} AppRotateBytes 5242880"; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} AppExit Default Restart"; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} AppRestartDelay 5000"; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#MyServiceName} Start SERVICE_AUTO_START"; Flags: runhidden waituntilterminated

; 3. Pornire serviciu imediat dupa instalare
Filename: "{app}\nssm.exe"; Parameters: "start {#MyServiceName}"; Flags: runhidden waituntilterminated

; 4. Deschidere panou admin in browserul implicit
Filename: "http://127.0.0.1:8002"; Description: "Deschide Panoul de Configurare Sincrona Bridge (8002)"; Flags: postinstall shellexec skipifsilent

[UninstallRun]
; Oprire si stergere serviciu la dezinstalare
Filename: "{app}\nssm.exe"; Parameters: "stop {#MyServiceName}"; Flags: runhidden waituntilterminated; RunOnceId: "StopService"
Filename: "{app}\nssm.exe"; Parameters: "remove {#MyServiceName} confirm"; Flags: runhidden waituntilterminated; RunOnceId: "RemoveService"

; Curatare exceptie Windows Defender la dezinstalare
Filename: "powershell.exe"; Parameters: "-ExecutionPolicy Bypass -WindowStyle Hidden -NoProfile -Command ""Remove-MpPreference -ExclusionPath '{app}'"""; Flags: runhidden waituntilterminated; RunOnceId: "CleanDefender"
